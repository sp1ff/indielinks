// Copyright (C) 2025-2026 Michael Herstine <sp1ff@pobox.com>
//
// This file is part of indielinks.
//
// indielinks is free software: you can redistribute it and/or modify it under the terms of the GNU
// General Public License as published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// indielinks is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
// even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License along with indielinks.  If not,
// see <http://www.gnu.org/licenses/>.

fn export_toml(
    ctx: &mut nickel_lang::Context,
    stack: &str,
    source_ncl: &str,
    target_toml: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expr = ctx
        .eval_deep_for_export(&format!(
            r#"let stacks = import "stacks.ncl" in (import "{source_ncl}") stacks."{stack}""#
        ))
        .map_err(|err| format!("{err:?}"))?;
    std::fs::write(
        target_toml,
        &ctx.expr_to_toml(&expr).map_err(|err| format!("{err:?}"))?,
    )
    .map_err(|err| err.into())
}

fn export_cluster_toml(
    ctx: &mut nickel_lang::Context,
    stack: &str,
    source_ncl: &str,
    backend: &str,
    target_dir: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let expr = ctx
        .eval_deep_for_export(&format!(
            r#"let stacks = import "stacks.ncl" in (import "{source_ncl}") stacks."{stack}""#
        ))
        .map_err(|err| format!("{err:#?}"))?;
    let arr = expr
        .as_array()
        .ok_or("Source Nickel didn't evaluate to an array?")?;
    arr.iter()
        .enumerate()
        .map(|(i, expr)| {
            ctx.expr_to_toml(&expr)
                .map_err(|err| format!("{err:#?}"))
                .and_then(|toml| {
                    std::fs::write(
                        format!("{target_dir}/indielinksd-{backend}-{i}.toml"),
                        &toml,
                    )
                    .map_err(|err| format!("{err:#?}"))
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.into())
        .map(|_| ())
}

fn configure_stack(
    ctx: &mut nickel_lang::Context,
    stack: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let target_directory = format!("../target/conf/{stack}/");
    std::fs::create_dir_all(&target_directory)?;

    export_toml(
        ctx,
        stack,
        "indielinksd-single-node-scylla.ncl",
        &format!("{target_directory}indielinksd-{stack}-scylla.toml"),
    )?;
    export_toml(
        ctx,
        stack,
        "indielinksd-single-node-alternator.ncl",
        &format!("{target_directory}indielinksd-{stack}-alternator.toml"),
    )?;

    export_cluster_toml(
        ctx,
        stack,
        "indielinksd-cluster-scylla.ncl",
        "scylla",
        &target_directory,
    )?;
    export_cluster_toml(
        ctx,
        stack,
        "indielinksd-cluster-alternator.ncl",
        "alternator",
        &target_directory,
    )
}

// `indielinks` build script
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate Rust bindings for the `indielinks` protobuf definitions.
    println!("cargo:rerun-if-changed=proto/indielinks.proto");
    tonic_build::configure().compile_protos_with_config(
        tonic_build::Config::default(),
        &["proto/indielinks.proto"],
        &["proto"],
    )?;
    // Use nickel to generate our configuration files. Nb. we just go ahead and generate them for
    // all stacks, regardless of the current git worktree. I'm doing this to ease the transition
    // from hand-authoring config files; that could change.
    [
        "mk-stack.ncl",
        "stacks.ncl",
        "alternator-backend.ncl",
        "scylla-backend.ncl",
        "indielinksd-common.ncl",
        "peppers.ncl",
        "indielinksd-front-matter.ncl",
        "indielinksd-front-matter-clustered.ncl",
        "indielinksd-single-node-alternator.ncl",
        "indielinksd-single-node-scylla.ncl",
        "indielinksd-cluster-scylla.ncl",
        "indielinksd-cluster-alternator.ncl",
    ]
    .into_iter()
    .for_each(|ncl| println!("cargo:rerun-if-changed=../conf/{ncl}"));

    let mut ctx = nickel_lang::Context::new().with_added_import_paths(vec!["../conf".into()]);
    // It's a bit irritating to have the list of stacks hard-coded; we should drive this off of,
    // well, off of a configuration file.
    ["master", "bugfix", "front-end", "pre-alpha"]
        .into_iter()
        .map(|stack| configure_stack(&mut ctx, stack))
        .collect::<Result<Vec<_>, _>>()
        .map(|_| ())
}
