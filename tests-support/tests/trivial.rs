// Copyright (C) 2026 Michael Herstine <sp1ff@pobox.com>
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

use {
    libtest_mimic::Failed,
    std::{convert::Infallible, process::ExitCode, result::Result as StdResult},
    tests_support::{
        async_integration_test, AsyncIntegrationTest, Error, IntegrationTest, TestConfiguration,
    },
};

use async_trait::async_trait;
use tests_support::Fixture;

#[derive(Debug)]
pub struct TrivialFixture {
    id: usize,
}

impl TrivialFixture {
    pub const fn new(id: usize) -> Self {
        Self { id }
    }
}

inventory::collect!(TrivialFixture);

inventory::submit!(TrivialFixture::new(1));
inventory::submit!(TrivialFixture::new(2));
inventory::submit!(TrivialFixture::new(3));

#[async_trait]
impl Fixture for TrivialFixture {
    type Error = Infallible;
    type Backend = ();
    /// The type of configuration salient to this sort of fixture
    type Configuration = ();
    type Id = usize;

    fn id(&self) -> Self::Id {
        self.id
    }
    /// Create this domain's "backend"
    async fn new_backend(&self, _: &Self::Configuration) -> StdResult<Self::Backend, Self::Error> {
        Ok(())
    }
    /// Setup this test fixture
    async fn setup(&self, _: &Self::Configuration) -> StdResult<(), Self::Error> {
        Ok(())
    }

    /// Teardown this test fixture
    async fn teardown(&self, _: &Self::Configuration) -> StdResult<(), Self::Error> {
        Ok(())
    }
}

fn trivial_test() -> StdResult<(), Failed> {
    assert!(1 + 1 == 2);
    Ok(())
}

struct TrivialTest {
    name: &'static str,
}

impl TrivialTest {
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }
}

inventory::collect!(TrivialTest);

inventory::submit!(TrivialTest::new("that one plus one equals two"));

impl IntegrationTest for TrivialTest {
    type F = TrivialFixture;
    fn germane(&self, id: <TrivialFixture as Fixture>::Id) -> bool {
        id == 1 || id == 3
    }
    fn name(&self) -> String {
        self.name.to_owned()
    }
}

#[async_trait]
impl AsyncIntegrationTest for TrivialTest {
    async fn run(&self, _: (), _: ()) -> StdResult<(), Failed> {
        trivial_test()
    }
}
fn main() -> StdResult<ExitCode, Error<TrivialFixture>> {
    async_integration_test(
        TestConfiguration::<TrivialFixture> {
            runner: Some(Default::default()),
            domain: (),
        },
        inventory::iter::<TrivialFixture>.into_iter(),
        inventory::iter::<TrivialTest>,
    )
}
