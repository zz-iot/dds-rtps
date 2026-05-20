const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const LogLevel = enum { err, warn, info, debug };

    const version = b.option([]const u8, "dds-version", "Full zzdds version string for the executable name (e.g. 0.1.0-zig.0.16.0); omit for a stable CI-friendly name");
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer (requires libc, Linux only)") orelse false;
    const default_log_level: LogLevel = switch (optimize) {
        .Debug => .debug,
        .ReleaseSafe, .ReleaseFast, .ReleaseSmall => .info,
    };
    const log_level = b.option(LogLevel, "log-level", "shape_main std.log level: err, warn, info, debug (default matches Zig build mode)") orelse default_log_level;

    const zzdds_dep = b.dependency("zzdds", .{ .target = target, .optimize = optimize });
    const zzdds_mod = zzdds_dep.module("zzdds");
    const zzdds_gen = zzdds_dep.module("zzdds_generated");

    // Build the "dds" shim module from our vendor implementation.
    // shape_main.zig imports only this module; it has no direct zzdds dependency.
    const dds_mod = b.createModule(.{
        .root_source_file = b.path("dds_impl.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "zzdds", .module = zzdds_mod },
            .{ .name = "zzdds_generated", .module = zzdds_gen },
        },
    });

    const exe_name = if (version) |v|
        std.fmt.allocPrint(b.allocator, "zzdds-{s}_shape_main_linux", .{v}) catch @panic("OOM")
    else
        "zzdds_shape_main_linux";
    const shape_main_options = b.addOptions();
    shape_main_options.addOption([]const u8, "log_level", @tagName(log_level));

    const exe = b.addExecutable(.{
        .name = exe_name,
        .root_module = b.createModule(.{
            .root_source_file = b.path("../shape_main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "dds", .module = dds_mod },
                .{ .name = "shape_main_options", .module = shape_main_options.createModule() },
            },
        }),
    });
    exe.root_module.link_libc = true;
    exe.root_module.sanitize_thread = sanitize_thread;
    b.installArtifact(exe);

    const run_step = b.step("run", "Run shape_main");
    const run_cmd = b.addRunArtifact(exe);
    if (b.args) |args| run_cmd.addArgs(args);
    run_step.dependOn(&run_cmd.step);
}
