const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const version = b.option([]const u8, "dds-version", "ZenzenDDS version string embedded in the executable name (default: 0.0.0)") orelse "0.0.0";
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer (requires libc, Linux only)") orelse false;

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

    const exe_name = std.fmt.allocPrint(b.allocator, "zenzen_dds-{s}_shape_main_linux", .{version}) catch @panic("OOM");

    const exe = b.addExecutable(.{
        .name = exe_name,
        .root_module = b.createModule(.{
            .root_source_file = b.path("../shape_main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "dds", .module = dds_mod },
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
