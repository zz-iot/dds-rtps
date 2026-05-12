//! Zig DDS shim — interface contract for srcZig/shape_main.zig.
//!
//! shape_main.zig imports a module named "dds".  Any Zig DDS vendor that
//! wants to participate in the dds-rtps interoperability test suite provides
//! their own implementation of this module and wires it up as the "dds"
//! dependency in their build.zig.
//!
//! ZenzenDDS's implementation lives in zenzen-zig/dds_impl.zig.
//!
//! ── Required exports ──────────────────────────────────────────────────────
//!
//!   pub const DDS = ...;
//!     Re-export of the vendor's standard DDS type package.  Must expose the
//!     standard DCPS entity handles and QoS/Status types used by shape_main:
//!     DomainParticipant, Publisher, Subscriber, Topic, ContentFilteredTopic,
//!     TopicDescription, DataWriter, DataReader, DataWriterQos, DataReaderQos,
//!     PublisherQos, SubscriberQos, DataWriterListener, DataReaderListener,
//!     StatusMask, and the status constants (OFFERED_INCOMPATIBLE_QOS_STATUS,
//!     etc.).
//!
//!   pub const Participant = struct { ... };
//!     Opaque vendor state that bundles transport, discovery, and factory.
//!     shape_main calls createParticipant / destroyParticipant and then
//!     calls toDDS() to get the standard DomainParticipant handle for use
//!     with the standard vtable API.
//!
//!   pub fn createParticipant(alloc: std.mem.Allocator, domain_id: u32) !*Participant;
//!   pub fn destroyParticipant(p: *Participant) void;
//!
//!   pub fn topicName(topic: DDS.Topic) []const u8;
//!     Returns the topic name string from a DDS.Topic handle.
//!
//!   ── DataWriter extras (not in the standard DCPS vtable) ───────────────
//!
//!   pub const WriteKind = enum { alive, dispose, unregister };
//!
//!   pub fn writeRaw(dw: DDS.DataWriter, kind: WriteKind,
//!                   key_hash: [16]u8, data: []const u8) !void;
//!     Write a pre-serialized CDR payload.  The vendor stamps the source
//!     timestamp internally (always "now") matching the behaviour of the
//!     standard typed write() call in C/C++/Rust shape_main implementations.
//!
//!   pub fn writerMatchedCount(dw: DDS.DataWriter) usize;
//!   pub fn writerNotifyDeadline(dw: DDS.DataWriter) void;
//!
//!   ── DataReader extras ─────────────────────────────────────────────────
//!
//!   pub const TakenSample = struct {
//!       data:  []u8,
//!       alloc: std.mem.Allocator,
//!       pub fn deinit(self: TakenSample) void,
//!   };
//!
//!   pub fn takeRaw(dr: DDS.DataReader) ?TakenSample;
//!     Returns the next pending sample, or null if the queue is empty.
//!     Caller must call sample.deinit() when done.
//!
//!   pub fn readerMatchedCount(dr: DDS.DataReader) usize;
//!   pub fn readerNotifyDeadline(dr: DDS.DataReader) void;
//!
//!   ── ContentFilteredTopic evaluation ──────────────────────────────────
//!
//!   pub const FilterValue = union(enum) {
//!       string: []const u8,
//!       int:    i64,
//!       float:  f64,
//!   };
//!
//!   pub const FieldAccessor = struct {
//!       ctx: *anyopaque,
//!       get: *const fn (ctx: *anyopaque, field: []const u8) ?FilterValue,
//!   };
//!
//!   pub fn cftMatchSample(cft: DDS.ContentFilteredTopic, acc: FieldAccessor) bool;
//!   pub fn cftTopicDescription(cft: DDS.ContentFilteredTopic) DDS.TopicDescription;

// This file is documentation only.  shape_main.zig imports the module
// named "dds" which is provided by the vendor's build.zig, not this file.
