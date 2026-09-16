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
//! the dark-theme plan, with individual values tuned during browser review.

use std::collections::HashMap;

use thaw::Theme;

const SYSTEM_FONT: &str = "'Segoe UI', 'Segoe UI Web (West European)', ui-sans-serif, system-ui, \
                          -apple-system, BlinkMacSystemFont, Roboto, 'Helvetica Neue', sans-serif";

/// The indielinks brand ramp, shared by both palettes.
fn brand_colors() -> HashMap<i32, &'static str> {
    HashMap::from([
        (10, "#061333"),
        (20, "#0B1F52"),
        (30, "#102B70"),
        (40, "#16388F"),
        (50, "#1944AD"),
        (60, "#1B4DC4"),
        (70, "#1D4ED8"),
        (80, "#2563EB"),
        (90, "#3B73EE"),
        (100, "#5385F1"),
        (110, "#6D98F4"),
        (120, "#88AAF6"),
        (130, "#A5BDF8"),
        (140, "#C0D0FA"),
        (150, "#DCE5FC"),
        (160, "#F1F5FE"),
    ])
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
        .set_color_neutral_background_1("#FFFFFF".to_owned());
    theme
        .color
        .set_color_neutral_background_1_hover("#F8FAFC".to_owned());
    theme
        .color
        .set_color_neutral_background_1_pressed("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_3("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_3_hover("#E9EDF2".to_owned());
    theme
        .color
        .set_color_neutral_background_3_pressed("#DEE4EB".to_owned());
    theme
        .color
        .set_color_neutral_background_4("#F8FAFC".to_owned());
    theme
        .color
        .set_color_neutral_background_4_hover("#F4F6F8".to_owned());
    theme
        .color
        .set_color_neutral_background_4_pressed("#E9EDF2".to_owned());

    theme
        .color
        .set_color_neutral_foreground_1("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_hover("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_pressed("#172033".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_hover("#465365".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_pressed("#344153".to_owned());
    theme
        .color
        .set_color_neutral_foreground_3("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_foreground_on_brand("#FFFFFF".to_owned());

    theme.color.set_color_neutral_stroke_1("#D8DEE8".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_hover("#BCC5D1".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_pressed("#9FAAB8".to_owned());
    theme.color.set_color_neutral_stroke_2("#D8DEE8".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible("#5F6B7A".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_hover("#465365".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_pressed("#344153".to_owned());

    theme.color.set_color_brand_background("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_background_hover("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_background_pressed("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_1("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_foreground_2("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_hover("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_pressed("#1D4ED8".to_owned());
    theme.color.set_color_brand_stroke_1("#2563EB".to_owned());
    theme.color.set_color_stroke_focus_2("#2563EB".to_owned());

    theme
        .color
        .set_color_neutral_shadow_ambient("rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_color_neutral_shadow_key("rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_shadow16("0 8px 24px rgb(23 32 51 / 12%)".to_owned());
    theme
        .color
        .set_shadow64("0 8px 24px rgb(23 32 51 / 12%)".to_owned());

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
        .set_color_neutral_background_1("#161E2B".to_owned());
    theme
        .color
        .set_color_neutral_background_1_hover("#1D2735".to_owned());
    theme
        .color
        .set_color_neutral_background_1_pressed("#242F40".to_owned());
    theme
        .color
        .set_color_neutral_background_3("#0F1520".to_owned());
    theme
        .color
        .set_color_neutral_background_3_hover("#161E2B".to_owned());
    theme
        .color
        .set_color_neutral_background_3_pressed("#1D2735".to_owned());
    theme
        .color
        .set_color_neutral_background_4("#1D2735".to_owned());
    theme
        .color
        .set_color_neutral_background_4_hover("#242F40".to_owned());
    theme
        .color
        .set_color_neutral_background_4_pressed("#2A3648".to_owned());

    theme
        .color
        .set_color_neutral_foreground_1("#EDF2F7".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_hover("#EDF2F7".to_owned());
    theme
        .color
        .set_color_neutral_foreground_1_pressed("#EDF2F7".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2("#A9B4C3".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_hover("#C3CDDA".to_owned());
    theme
        .color
        .set_color_neutral_foreground_2_pressed("#D7DEE8".to_owned());
    theme
        .color
        .set_color_neutral_foreground_3("#A9B4C3".to_owned());
    theme
        .color
        .set_color_neutral_foreground_on_brand("#FFFFFF".to_owned());

    theme.color.set_color_neutral_stroke_1("#344154".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_hover("#42506B".to_owned());
    theme
        .color
        .set_color_neutral_stroke_1_pressed("#536284".to_owned());
    theme.color.set_color_neutral_stroke_2("#344154".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible("#A9B4C3".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_hover("#C3CDDA".to_owned());
    theme
        .color
        .set_color_neutral_stroke_accessible_pressed("#D7DEE8".to_owned());

    // Primary actions stay recognizably blue with white text; links & other brand foregrounds
    // move up the brand ramp to retain contrast against dark surfaces.
    theme.color.set_color_brand_background("#2563EB".to_owned());
    theme
        .color
        .set_color_brand_background_hover("#3B73EE".to_owned());
    theme
        .color
        .set_color_brand_background_pressed("#1D4ED8".to_owned());
    theme
        .color
        .set_color_brand_background_2("#1A2740".to_owned());
    theme
        .color
        .set_color_brand_foreground_1("#88AAF6".to_owned());
    theme
        .color
        .set_color_brand_foreground_2("#A5BDF8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link("#88AAF6".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_hover("#A5BDF8".to_owned());
    theme
        .color
        .set_color_brand_foreground_link_pressed("#C0D0FA".to_owned());
    theme.color.set_color_brand_stroke_1("#88AAF6".to_owned());
    theme.color.set_color_stroke_focus_2("#88AAF6".to_owned());

    // Darker, lower-opacity shadows, so overlays separate from the canvas without a light halo.
    theme
        .color
        .set_color_neutral_shadow_ambient("rgb(0 0 0 / 40%)".to_owned());
    theme
        .color
        .set_color_neutral_shadow_key("rgb(0 0 0 / 40%)".to_owned());
    theme
        .color
        .set_shadow16("0 8px 24px rgb(0 0 0 / 40%)".to_owned());
    theme
        .color
        .set_shadow64("0 8px 24px rgb(0 0 0 / 40%)".to_owned());

    theme
}
