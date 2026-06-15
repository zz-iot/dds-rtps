//! ZenzenDDS implementation of the srcZig/dds shim protocol.
//!
//! shape_main.zig imports this module as "dds".  Every symbol exported here
//! matches the protocol documented in srcZig/dds.zig; see that file for the
//! full contract that any Zig DDS vendor must satisfy.
//!
//! CDR serialization and key-hash computation are NOT handled here.  They live
//! in the zidl-generated shape.zig (imported by shape_main.zig as "shape_gen").
//! This module only exposes raw RTPS plumbing: writeCdr / takeCdr and the DDS
//! entity-management functions used by shape_main.zig.

const std = @import("std");

const zzdds = @import("zzdds");
const zzdds_gen = @import("zzdds_generated");

pub const DDS = zzdds_gen.DDS;

// Type aliases required by the generated ShapeTypeDataWriter / ShapeTypeDataReader.
pub const DataWriter = DDS.DataWriter;
pub const DataReader = DDS.DataReader;
pub const InstanceStateKind = DDS.InstanceStateKind;
pub const InstanceHandle_t = DDS.InstanceHandle_t;

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

    const dp = dpf.create_participant(domain_id, .{}, null, 0);
    if (dp.ptr == nil.NIL_PTR) return error.ParticipantFailed;

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

/// Write a pre-serialized CDR payload.  Called by the generated ShapeTypeDataWriter
/// and also directly by shape_main for raw dispose/unregister payloads.
pub fn writeCdr(
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

/// Raw sample returned by takeCdr.  Caller must call deinit() when done.
pub const RawSample = struct {
    data: []u8,
    alloc: std.mem.Allocator,
    instance_state: DDS.InstanceStateKind,
    instance_handle: DDS.InstanceHandle_t,

    pub fn deinit(self: RawSample) void {
        self.alloc.free(self.data);
    }
};

/// Pop the next pending sample from dr, or return null if the queue is empty.
/// Called by the generated ShapeTypeDataReader and directly by shape_main for
/// NOT_ALIVE sample handling (which needs the raw payload to extract the key).
pub fn takeCdr(dr: DDS.DataReader) ?RawSample {
    const impl: *DataReaderImpl = @ptrCast(@alignCast(dr.ptr));
    const taken = impl.takeRaw() orelse return null;
    return .{
        .data = taken.data,
        .alloc = impl.alloc,
        .instance_state = taken.info.instance_state,
        .instance_handle = taken.info.instance_handle,
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
    _ = impl.registerTypeSupport(type_name, ts);
}

// ── Nil sentinel helpers ──────────────────────────────────────────────────────
// All nil entities share the same underlying nil_storage address (NIL_PTR).

pub fn nilTopicListener() DDS.TopicListener {
    return DDS.noop_TopicListener;
}
pub fn nilPublisherListener() DDS.PublisherListener {
    return DDS.noop_PublisherListener;
}
pub fn nilSubscriberListener() DDS.SubscriberListener {
    return DDS.noop_SubscriberListener;
}

pub fn isNilDp(dp: DDS.DomainParticipant) bool {
    return dp.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilTopic(t: DDS.Topic) bool {
    return t.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilPub(p: DDS.Publisher) bool {
    return p.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilSub(s: DDS.Subscriber) bool {
    return s.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilDw(dw: DDS.DataWriter) bool {
    return dw.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilDr(dr: DDS.DataReader) bool {
    return dr.ptr == zzdds.dcps.NIL_PTR;
}
pub fn isNilCft(cft: DDS.ContentFilteredTopic) bool {
    return cft.ptr == zzdds.dcps.NIL_PTR;
}
