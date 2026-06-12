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
// see <http://www.gnu.org/licenses/>.

//! # fin - Finite Sets
//!
//! ## Introduction
//!
//! [`Fin<N>`] is the type of all natural numbers strictly less than `N` (so `Fin<2>` = {0, 1}).
//! They're all over the place in dependently-typed languages like Rocq & Idris, where they're used
//! to do things like write infallible index operators (to name just one example). For instance, the
//! Idris standard library has:
//!
//! [Fin<N>]: Fin
//!
//! ```text
//! index' : (xs : List a) -> Fin (length xs) -> a
//! ```
//!
//! Given a list `xs` of `a`s, and a `Fin (length xs)`, return the element at that index with no
//! error case.
//!
//! Sadly, we can't quite do all that in Rust, but with some help from the [typenum] crate, we can
//! get partway there. We have correctness by construction:
//!
//! ```
//! use typenum::U3;
//! use indielinks_shared::fin::Fin;
//! let x: Option<Fin<U3>> = Fin::new(4);
//! assert!(x.is_none());
//! ```
//!
//! If you _have_ a [Fin], you can always weaken it (i.e. get a numerically equivalent instance of
//! the finite set whose bound is one element higher):
//!
//! ```
//! use typenum::{U3, U4};
//! use indielinks_shared::fin::Fin;
//! let x: Fin<U3> = Fin::new(2).unwrap();
//! let y : Fin<U4> = x.weaken();
//! assert!(x.get() == y.get());
//! ```
//!
//! and so on. I've implemented many of the methods found in the Idris implementation. Nb that `Fin<0>`
//! _is_ a type, but it's empty.
// One thing that would be nice to add would be a macro that allows you to create a `Fin` from a
// compile-time const and fails the compilation if it's invalid.

use std::{
    marker::PhantomData,
    ops::{Add, Sub},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use typenum::{Diff, Sum, U1, Unsigned};

/// FINite Set: the set of naturals less than or equal to `N`
#[derive(Clone, Debug)]
pub struct Fin<N: Unsigned> {
    val: usize,
    _phantom: PhantomData<N>,
}

// The derived `Serialize` & `Deserialize` implementations are not suitable.
impl<N: Unsigned> Serialize for Fin<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.val.serialize(serializer)
    }
}

impl<'de, N: Unsigned> Deserialize<'de> for Fin<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = usize::deserialize(deserializer)?;
        Fin::<N>::new(val).ok_or(<D::Error as serde::de::Error>::custom("Out of range"))
    }
}

impl<N: Unsigned> Fin<N> {
    /// Create a new `Fin<N>`; if `val` is greater than or equal to `N`, return `None`
    pub fn new(val: usize) -> Option<Self> {
        if val < N::USIZE {
            Some(Self {
                val,
                _phantom: PhantomData,
            })
        } else {
            None
        }
    }
    /// Add a natural number to this `Fin`, adjusting the bound accordingly (infallible)
    pub fn shift<M: Unsigned + Add<N>>(&self, _: M) -> Fin<Sum<M, N>>
    where
        <M as Add<N>>::Output: Unsigned,
    {
        Fin::<Sum<M, N>> {
            val: self.val + M::USIZE,
            _phantom: PhantomData,
        }
    }
    /// Retreive this `Fin`'s value, typed as a `usize`
    pub fn get(&self) -> usize {
        self.val
    }
}

impl<N: Unsigned + Add<U1>> Fin<N>
where
    <N as Add<U1>>::Output: Unsigned,
{
    /// Weaken this `Fin`; i.e. produce a numerically equivalent `Fin` whose bound is one greater
    /// than this `Fin`-- infallible.
    pub fn weaken(&self) -> Fin<Sum<N, U1>> {
        Fin::<Sum<N, U1>> {
            val: self.val,
            _phantom: PhantomData,
        }
    }
}

impl<N: Unsigned + Sub<U1>> Fin<N>
where
    <N as Sub<U1>>::Output: Unsigned,
{
    /// For a given `N`, return the largest `Fin` bounded by it
    pub fn last() -> Self {
        Self {
            val: Diff::<N, U1>::USIZE,
            _phantom: PhantomData,
        }
    }
    /// Attempt to strengthen this `Fin`; i.e. produce a numerically equivalent `Fin` whose bound
    /// is one _less_. If that's impossible, return `None`.
    pub fn strengthen(&self) -> Option<Fin<Diff<N, U1>>> {
        if self.val < Diff::<N, U1>::USIZE {
            Some(Fin::<Diff<N, U1>> {
                val: self.val,
                _phantom: PhantomData,
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use typenum::{U1, U2, U3, U4, U6};

    use super::*;

    #[test]
    fn smoke_test() {
        let x: Option<Fin<U3>> = Fin::new(4);
        assert!(x.is_none());

        let x: Option<Fin<U3>> = Fin::new(1);
        assert!(x.is_some());
        let x = x.unwrap();

        let y: Fin<U4> = x.weaken();
        assert!(y.get() == 1);

        let z: Option<Fin<U3>> = y.strengthen();
        assert!(z.unwrap().get() == 1);

        assert!(Fin::<U1>::new(0).unwrap().strengthen().is_none());

        let lst = Fin::<U3>::last();
        assert!(lst.get() == 2);

        let shft: Fin<U6> = y.shift(U2::new());
        assert!(shft.val == 3);

        // Cool! Won't compile!
        // let _ = Fin::<U0>::last();
    }
}
