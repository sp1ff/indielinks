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

//! # The light & dark palettes
//!
//! ## Introduction
//!
//! Both palettes are built from the same brand ramp and share typography, radii & timing
//! customization ([customize]); only color and shadow choices differ. [light] is the canonical
//! indielinks appearance: its values must remain unchanged. [dark] targets the design laid out in
//! the dark-theme plan, with individual values tuned during browser review. Palette values live in
//! `indielinks-shared/assets/theme.css`; this module gives Thaw references to those CSS tokens.

use std::collections::HashMap;

use thaw::Theme;

const SYSTEM_FONT: &str = "'Segoe UI', 'Segoe UI Web (West European)', ui-sans-serif, system-ui, \
                          -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif";

const BRAND_COLORS: [(i32, &str); 16] = [
    (10, "var(--indielinks-brand-10)"),
    (20, "var(--indielinks-brand-20)"),
    (30, "var(--indielinks-brand-30)"),
    (40, "var(--indielinks-brand-40)"),
    (50, "var(--indielinks-brand-50)"),
    (60, "var(--indielinks-brand-60)"),
    (70, "var(--indielinks-brand-70)"),
    (80, "var(--indielinks-brand-80)"),
    (90, "var(--indielinks-brand-90)"),
    (100, "var(--indielinks-brand-100)"),
    (110, "var(--indielinks-brand-110)"),
    (120, "var(--indielinks-brand-120)"),
    (130, "var(--indielinks-brand-130)"),
    (140, "var(--indielinks-brand-140)"),
    (150, "var(--indielinks-brand-150)"),
    (160, "var(--indielinks-brand-160)"),
];

fn token(name: &str) -> String {
    format!("var(--indielinks-{name})")
}

/// The indielinks brand ramp, shared by both palettes.
fn brand_colors() -> HashMap<i32, &'static str> {
    HashMap::from(BRAND_COLORS)
}

/// Apply the typography, radii & timing customization common to both palettes.
fn customize(theme: &mut Theme) {
    theme.common.set_font_family_base(SYSTEM_FONT.to_owned());
    theme.common.set_font_size_base_400("15px".to_owned());
    theme.common.set_border_radius_small("4px".to_owned());
    theme.common.set_border_radius_medium("4px".to_owned());
    theme.common.set_border_radius_large("6px".to_owned());
    theme.common.set_border_radius_x_large("8px".to_owned());
    theme.common.set_duration_ultra_fast("120ms".to_owned());
    theme.common.set_duration_faster("150ms".to_owned());
    theme.common.set_duration_normal("180ms".to_owned());
    theme.common.set_duration_gentle("220ms".to_owned());
    theme.common.set_duration_slow("250ms".to_owned());
}

/// Construct the indielinks light theme.
pub fn light() -> Theme {
    let mut theme = Theme::custom_light(&brand_colors());
    customize(&mut theme);

    theme
        .color
        .set_color_neutral_background_1(token("background-1"));
    theme
        .color
        .set_color_neutral_background_1_hover(token("background-1-hover"));
    theme
        .color
        .set_color_neutral_background_1_pressed(token("background-1-pressed"));
    theme
        .color
        .set_color_neutral_background_3(token("background-3"));
    theme
        .color
        .set_color_neutral_background_3_hover(token("background-3-hover"));
    theme
        .color
        .set_color_neutral_background_3_pressed(token("background-3-pressed"));
    theme
        .color
        .set_color_neutral_background_4(token("background-4"));
    theme
        .color
        .set_color_neutral_background_4_hover(token("background-4-hover"));
    theme
        .color
        .set_color_neutral_background_4_pressed(token("background-4-pressed"));

    theme
        .color
        .set_color_neutral_foreground_1(token("foreground-1"));
    theme
        .color
        .set_color_neutral_foreground_1_hover(token("foreground-1-hover"));
    theme
        .color
        .set_color_neutral_foreground_1_pressed(token("foreground-1-pressed"));
    theme
        .color
        .set_color_neutral_foreground_2(token("foreground-2"));
    theme
        .color
        .set_color_neutral_foreground_2_hover(token("foreground-2-hover"));
    theme
        .color
        .set_color_neutral_foreground_2_pressed(token("foreground-2-pressed"));
    theme
        .color
        .set_color_neutral_foreground_3(token("foreground-3"));
    theme
        .color
        .set_color_neutral_foreground_on_brand(token("foreground-on-brand"));

    theme.color.set_color_neutral_stroke_1(token("stroke-1"));
    theme
        .color
        .set_color_neutral_stroke_1_hover(token("stroke-1-hover"));
    theme
        .color
        .set_color_neutral_stroke_1_pressed(token("stroke-1-pressed"));
    theme.color.set_color_neutral_stroke_2(token("stroke-2"));
    theme
        .color
        .set_color_neutral_stroke_accessible(token("stroke-accessible"));
    theme
        .color
        .set_color_neutral_stroke_accessible_hover(token("stroke-accessible-hover"));
    theme
        .color
        .set_color_neutral_stroke_accessible_pressed(token("stroke-accessible-pressed"));

    theme
        .color
        .set_color_brand_background(token("brand-background"));
    theme
        .color
        .set_color_brand_background_hover(token("brand-background-hover"));
    theme
        .color
        .set_color_brand_background_pressed(token("brand-background-pressed"));
    theme
        .color
        .set_color_brand_foreground_1(token("brand-foreground-1"));
    theme
        .color
        .set_color_brand_foreground_2(token("brand-foreground-2"));
    theme
        .color
        .set_color_brand_foreground_link(token("brand-link"));
    theme
        .color
        .set_color_brand_foreground_link_hover(token("brand-link-hover"));
    theme
        .color
        .set_color_brand_foreground_link_pressed(token("brand-link-pressed"));
    theme
        .color
        .set_color_brand_stroke_1(token("brand-stroke-1"));
    theme.color.set_color_stroke_focus_2(token("focus"));

    theme
        .color
        .set_color_neutral_shadow_ambient(token("shadow"));
    theme.color.set_color_neutral_shadow_key(token("shadow"));
    theme.color.set_shadow16(token("shadow"));
    theme.color.set_shadow64(token("shadow"));

    theme
}

/// Construct the indielinks dark theme.
pub fn dark() -> Theme {
    let mut theme = Theme::custom_dark(&brand_colors());
    customize(&mut theme);

    // Surfaces: a blue-tinted canvas, raised surfaces one step lighter, and subtle regions one
    // step lighter still. Hover & pressed states move *up* the lightness scale, the reverse of
    // the light palette.
    theme
        .color
        .set_color_neutral_background_1(token("background-1"));
    theme
        .color
        .set_color_neutral_background_1_hover(token("background-1-hover"));
    theme
        .color
        .set_color_neutral_background_1_pressed(token("background-1-pressed"));
    theme
        .color
        .set_color_neutral_background_3(token("background-3"));
    theme
        .color
        .set_color_neutral_background_3_hover(token("background-3-hover"));
    theme
        .color
        .set_color_neutral_background_3_pressed(token("background-3-pressed"));
    theme
        .color
        .set_color_neutral_background_4(token("background-4"));
    theme
        .color
        .set_color_neutral_background_4_hover(token("background-4-hover"));
    theme
        .color
        .set_color_neutral_background_4_pressed(token("background-4-pressed"));

    theme
        .color
        .set_color_neutral_foreground_1(token("foreground-1"));
    theme
        .color
        .set_color_neutral_foreground_1_hover(token("foreground-1-hover"));
    theme
        .color
        .set_color_neutral_foreground_1_pressed(token("foreground-1-pressed"));
    theme
        .color
        .set_color_neutral_foreground_2(token("foreground-2"));
    theme
        .color
        .set_color_neutral_foreground_2_hover(token("foreground-2-hover"));
    theme
        .color
        .set_color_neutral_foreground_2_pressed(token("foreground-2-pressed"));
    theme
        .color
        .set_color_neutral_foreground_3(token("foreground-3"));
    theme
        .color
        .set_color_neutral_foreground_on_brand(token("foreground-on-brand"));

    theme.color.set_color_neutral_stroke_1(token("stroke-1"));
    theme
        .color
        .set_color_neutral_stroke_1_hover(token("stroke-1-hover"));
    theme
        .color
        .set_color_neutral_stroke_1_pressed(token("stroke-1-pressed"));
    theme.color.set_color_neutral_stroke_2(token("stroke-2"));
    theme
        .color
        .set_color_neutral_stroke_accessible(token("stroke-accessible"));
    theme
        .color
        .set_color_neutral_stroke_accessible_hover(token("stroke-accessible-hover"));
    theme
        .color
        .set_color_neutral_stroke_accessible_pressed(token("stroke-accessible-pressed"));

    // Primary actions stay recognizably blue with white text; links & other brand foregrounds
    // move up the brand ramp to retain contrast against dark surfaces.
    theme
        .color
        .set_color_brand_background(token("brand-background"));
    theme
        .color
        .set_color_brand_background_hover(token("brand-background-hover"));
    theme
        .color
        .set_color_brand_background_pressed(token("brand-background-pressed"));
    theme
        .color
        .set_color_brand_background_2(token("brand-background-2"));
    theme
        .color
        .set_color_brand_foreground_1(token("brand-foreground-1"));
    theme
        .color
        .set_color_brand_foreground_2(token("brand-foreground-2"));
    theme
        .color
        .set_color_brand_foreground_link(token("brand-link"));
    theme
        .color
        .set_color_brand_foreground_link_hover(token("brand-link-hover"));
    theme
        .color
        .set_color_brand_foreground_link_pressed(token("brand-link-pressed"));
    theme
        .color
        .set_color_brand_stroke_1(token("brand-stroke-1"));
    theme.color.set_color_stroke_focus_2(token("focus"));

    // Darker, lower-opacity shadows, so overlays separate from the canvas without a light halo.
    theme
        .color
        .set_color_neutral_shadow_ambient(token("shadow"));
    theme.color.set_color_neutral_shadow_key(token("shadow"));
    theme.color.set_shadow16(token("shadow"));
    theme.color.set_shadow64(token("shadow"));

    theme
}
