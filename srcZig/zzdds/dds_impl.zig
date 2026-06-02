//! ZenzenDDS implementation of the srcZig/dds shim protocol.
//!
//! shape_main.zig imports this module as "dds".  Every symbol exported here
//! matches the protocol documented in srcZig/dds.zig; see that file for the
//! full contract that any Zig DDS vendor must satisfy.

const std = @import("std");

const zzdds = @import("zzdds");
const zzdds_gen = @import("zzdds_generated");

pub const DDS = zzdds_gen.DDS;

const UdpTransport = zzdds.udp_transport.UdpTransport;
const SpdpSedpDiscovery = zzdds.combined_discovery.SpdpSedpDiscovery;
const DomainParticipantFactoryImpl = zzdds.dcps.DomainParticipantFactoryImpl;
const DomainParticipantImpl = zzdds.dcps.DomainParticipantImpl;
const DataWriterImpl = zzdds.dcps.DataWriterImpl;
const DataReaderImpl = zzdds.dcps.DataReaderImpl;
const TopicImpl = zzdds.dcps.TopicImpl;
const ContentFilteredTopicImpl = zzdds.dcps.ContentFilteredTopicImpl;
const noop_security = zzdds.noop_security.noop_security_plugins;
const time_mod = zzdds.util.time;
const history_mod = zzdds.rtps.history;
const nil = zzdds.dcps;
const filter_mod = zzdds.dcps.filter;

// ── Participant bootstrapping ─────────────────────────────────────────────────

pub const Participant = struct {
    alloc: std.mem.Allocator,
    udp: *UdpTransport,
    disc: *SpdpSedpDiscovery,
    factory: *DomainParticipantFactoryImpl,
    dp: DDS.DomainParticipant,

    pub fn toDDS(self: *Participant) DDS.DomainParticipant {
        return self.dp;
    }
};

pub fn createParticipant(alloc: std.mem.Allocator, domain_id: u32) !*Participant {
    const p = try alloc.create(Participant);
    errdefer alloc.destroy(p);

    const udp = try UdpTransport.init(alloc, .{}, domain_id, null);
    errdefer udp.deinit();
    const transport = udp.transport();

    const disc = try SpdpSedpDiscovery.init(alloc, transport, domain_id, 3_000);
    errdefer disc.deinit();
    const discovery = disc.toDiscovery();

    var factory = try DomainParticipantFactoryImpl.init(
        alloc,
        transport,
        discovery,
        noop_security,
        .spec_random,
        .{},
    );
    errdefer factory.deinit();
    const dpf = factory.toDDSFactory();

    const dp = dpf.create_participant(domain_id, .{}, nil.nil_dp_listener, 0);
    if (dp.ptr == nil.nil_dp_listener.ptr) return error.ParticipantFailed;

    p.* = .{
        .alloc = alloc,
        .udp = udp,
        .disc = disc,
        .factory = factory,
        .dp = dp,
    };
    return p;
}

pub fn destroyParticipant(p: *Participant) void {
    const dpf = p.factory.toDDSFactory();
    _ = dpf.delete_participant(p.dp);
    p.factory.deinit();
    p.disc.deinit();
    p.udp.deinit();
    p.alloc.destroy(p);
}

// ── Topic name ────────────────────────────────────────────────────────────────

pub fn topicName(topic: DDS.Topic) []const u8 {
    const impl: *TopicImpl = @ptrCast(@alignCast(topic.ptr));
    return impl.topic_name;
}

// ── DataWriter extras ─────────────────────────────────────────────────────────

pub const WriteKind = enum { alive, dispose, unregister };

pub fn writeRaw(
    dw: DDS.DataWriter,
    kind: WriteKind,
    key_hash: [16]u8,
    data: []const u8,
) !void {
    const impl: *DataWriterImpl = @ptrCast(@alignCast(dw.ptr));
    const ck: history_mod.ChangeKind = switch (kind) {
        .alive => .alive,
        .dispose => .not_alive_disposed,
        .unregister => .not_alive_unregistered,
    };
    _ = try impl.writeRaw(ck, time_mod.RtpsTimestamp.now(), history_mod.INSTANCE_HANDLE_NIL, key_hash, data);
}

pub fn writerMatchedCount(dw: DDS.DataWriter) usize {
    const impl: *DataWriterImpl = @ptrCast(@alignCast(dw.ptr));
    return impl.matchedReaderCount();
}

pub fn writerNotifyDeadline(dw: DDS.DataWriter) void {
    const impl: *DataWriterImpl = @ptrCast(@alignCast(dw.ptr));
    impl.notifyDeadlineMissed();
}

// ── DataReader extras ─────────────────────────────────────────────────────────

pub const TakenSample = struct {
    data: []u8,
    alloc: std.mem.Allocator,
    instance_state: DDS.InstanceStateKind,

    pub fn deinit(self: TakenSample) void {
        self.alloc.free(self.data);
    }
};

pub fn takeRaw(dr: DDS.DataReader) ?TakenSample {
    const impl: *DataReaderImpl = @ptrCast(@alignCast(dr.ptr));
    const taken = impl.takeRaw() orelse return null;
    return .{
        .data = taken.data,
        .alloc = impl.alloc,
        .instance_state = taken.info.instance_state,
    };
}

pub fn readerMatchedCount(dr: DDS.DataReader) usize {
    const impl: *DataReaderImpl = @ptrCast(@alignCast(dr.ptr));
    return impl.matchedWriterCount();
}

pub fn readerNotifyDeadline(dr: DDS.DataReader) void {
    const impl: *DataReaderImpl = @ptrCast(@alignCast(dr.ptr));
    impl.notifyDeadlineMissed();
}

// ── ContentFilteredTopic evaluation ──────────────────────────────────────────

pub const FilterValue = filter_mod.FilterValue;
pub const FieldAccessor = filter_mod.FieldAccessor;

pub fn cftMatchSample(cft: DDS.ContentFilteredTopic, acc: FieldAccessor) bool {
    const impl: *ContentFilteredTopicImpl = @ptrCast(@alignCast(cft.ptr));
    return impl.matchSample(acc);
}

pub fn cftTopicDescription(cft: DDS.ContentFilteredTopic) DDS.TopicDescription {
    const impl: *ContentFilteredTopicImpl = @ptrCast(@alignCast(cft.ptr));
    return impl.toTopicDescription();
}

// ── TypeSupport ───────────────────────────────────────────────────────────────

pub const TypeSupport = zzdds.dcps.TypeSupport;

pub fn registerTypeSupport(
    dp: DDS.DomainParticipant,
    type_name: []const u8,
    ts: TypeSupport,
) void {
    const impl: *DomainParticipantImpl = @ptrCast(@alignCast(dp.ptr));
    impl.registerTypeSupport(type_name, ts);
}

// ── Nil sentinel helpers ──────────────────────────────────────────────────────
// All nil entities share the same underlying nil_storage address (NIL_PTR).
// We recover that address from any exported nil constant without needing to
// re-export NIL_PTR itself.

fn nilPtr() *anyopaque {
    return zzdds.dcps.nil_topic_listener.ptr;
}

pub fn nilTopicListener() DDS.TopicListener {
    return zzdds.dcps.nil_topic_listener;
}
pub fn nilPublisherListener() DDS.PublisherListener {
    return zzdds.dcps.nil_pub_listener;
}
pub fn nilSubscriberListener() DDS.SubscriberListener {
    return zzdds.dcps.nil_sub_listener;
}

pub fn isNilDp(dp: DDS.DomainParticipant) bool {
    return dp.ptr == nilPtr();
}
pub fn isNilTopic(t: DDS.Topic) bool {
    return t.ptr == nilPtr();
}
pub fn isNilPub(p: DDS.Publisher) bool {
    return p.ptr == nilPtr();
}
pub fn isNilSub(s: DDS.Subscriber) bool {
    return s.ptr == nilPtr();
}
pub fn isNilDw(dw: DDS.DataWriter) bool {
    return dw.ptr == nilPtr();
}
pub fn isNilDr(dr: DDS.DataReader) bool {
    return dr.ptr == nilPtr();
}
pub fn isNilCft(cft: DDS.ContentFilteredTopic) bool {
    return cft.ptr == nilPtr();
}
