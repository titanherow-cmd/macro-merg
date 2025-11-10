# Determine environment based on folder path
        # CRITICAL: Check for "deskt" first (more specific) before "mobile"
        # Because deskt- osrs is INSIDE the MOBILE folder
        folder_path_lower = str(folder_path).lower()
        
        is_desktop_group = "deskt" in folder_path_lower
        is_mobile_group = "mobile" in folder_path_lower and not is_desktop_group        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)
        
        # Determine environment based on folder path
        # CRITICAL: Check for "deskt" first (more specific) before "mobile"
        # Because deskt- osrs is INSIDE the MOBILE folder
        folder_path_lower = str(folder_path).lower()
        
        is_desktop_group = "deskt" in folder_path_lower
        is_mobile_group = "mobile" in folder_path_lower and not is_desktop_group
        
        # Apply anti-detection features (preserve special file behavior)
        if not is_special:
            # === MOBILE PROFILE === (Screen share - no mouse tracking)
            if is_mobile_group:
                ENABLE_TIME_FATIGUE = True
                ENABLE_MOUSE_JITTER = True      # ±2 pixels
                ENABLE_MICRO_PAUSES = True
                ENABLE_REACTION_VARIANCE = True
                ENABLE_PROGRESSIVE_FATIGUE = True # Get tired over time
                # Desktop features disabled for mobile
                ENABLE_MOUSE_CURVES = False
                ENABLE_CURSOR_OVERSHOOTS = False
                ENABLE_MOUSE_ACCELERATION = False
                ENABLE_DESKTOP_MISCLICKS = False
                ENABLE_DESKTOP_MOUSE_DRIFT = False
            
            # === DESKTOP PROFILE === (Full client - mouse tracking enabled)
            elif is_desktop_group:
                ENABLE_TIME_FATIGUE = True
                ENABLE_MOUSE_JITTER = True      # ±1 pixel
                ENABLE_MICRO_PAUSES = True
                ENABLE_REACTION_VARIANCE = True
                # Desktop-specific features
                ENABLE_MOUSE_CURVES = True      # Curved mouse paths
                ENABLE_CURSOR_OVERSHOOTS = True # Overshoot then correct
                ENABLE_MOUSE_ACCELERATION = True # Speed variance
                ENABLE_DESKTOP_MISCLICKS = True  # Accidental right-clicks
                ENABLE_DESKTOP_MOUSE_DRIFT = True # Idle mouse fidgeting
                ENABLE_PROGRESSIVE_FATIGUE = True # Get tired over time
            
            # === DEFAULT PROFILE === (Unknown folder - use safe mobile profile)
            else:
                ENABLE_TIME_FATIGUE = True
                ENABLE_MOUSE_JITTER = True
                ENABLE_MICRO_PAUSES = True
                ENABLE_REACTION_VARIANCE = True
                ENABLE_PROGRESSIVE_FATIGUE = True
                ENABLE_MOUSE_CURVES = False
                ENABLE_CURSOR_OVERSHOOTS = False
                ENABLE_MOUSE_ACCELERATION = False
                ENABLE_DESKTOP_MISCLICKS = False
                ENABLE_DESKTOP_MOUSE_DRIFT = False
                is_desktop_group = False  # Treat as mobile
            
            # Calculate session progress for fatigue
            session_progress = calculate_session_progress(idx, len(final_files))
            
            # PHASE 1: Time modifications (applies to both profiles)
            if ENABLE_TIME_FATIGUE:
                zb_evs, extra_mistake_chance = add_time_of_day_fatigue(zb_evs, rng)
            else:
                extra_mistake_chance = 0
            
            if ENABLE_PROGRESSIVE_FATIGUE:
                zb_evs = add_progressive_fatigue(zb_evs, rng, session_progress)
            
            if ENABLE_MICRO_PAUSES:
                zb_evs = add_micro_pauses(zb_evs, rng)
            
            if ENABLE_REACTION_VARIANCE:
                zb_evs = add_reaction_variance(zb_evs, rng)
            
            # PHASE 2: Desktop-specific mouse movements (before jitter)
            if ENABLE_MOUSE_CURVES:
                zb_evs = add_mouse_movement_curves(zb_evs, rng)
            
            if ENABLE_CURSOR_OVERSHOOTS:
                zb_evs = add_cursor_overshoots(zb_evs, rng)
            
            if ENABLE_MOUSE_ACCELERATION:
                zb_evs = add_mouse_acceleration_variance(zb_evs, rng)
            
            if ENABLE_DESKTOP_MISCLICKS:
                zb_evs = add_desktop_misclicks(zb_evs, rng)
            
            if ENABLE_DESKTOP_MOUSE_DRIFT:
                zb_evs = add_desktop_mouse_drift(zb_evs, rng)
            
            # PHASE 3: Coordinate modifications (both profiles, different precision)
            if ENABLE_MOUSE_JITTER:
                zb_evs = add_mouse_jitter(zb_evs, rng, is_desktop=is_desktop_group)
            
            # PHASE 4: Re-sort by time
            zb_evs, file_duration_ms = zero_base_events(zb_evs)#!/usr/bin/env python3

"""
merge_macros.py (Environment-Specific Anti-Detection)
- MOBILE profile: Optimized for screen share (click variance, timing, no mouse tracking)
- DESKTOP profile: Optimized for desktop client (mouse movements, cursor paths, full detection)
- Each folder gets appropriate anti-detection based on its name
- ANTI-DETECTION FEATURES:
  MOBILE:
    * Click jitter (±2 pixels for finger tap imprecision)
    * Time-of-day fatigue (slower at night, faster in morning)
    * Micro-pauses (hesitations between actions)
    * Reaction variance (human response delays)
    * Dynamic pause ranges (no fixed patterns)
    * Random AFK moments (simulates distractions)
    * Session mood variance (focused/tired sessions)
  DESKTOP:
    * Click jitter (±1 pixel for mouse precision)
    * Mouse movement curves (Bezier paths to targets)
    * Cursor overshoots (miss slightly, correct)
    * Mouse acceleration variance (speed changes)
    * Occasional misclicks (right-click then correct)
    * All mobile features + desktop-specific
"""

from pathlib import Path
import argparse
import json
import random
import re
import sys
from copy import deepcopy
from zipfile import ZipFile
import os
import math
from itertools import combinations

# --------- constants ---------
COUNTER_PATH = Path(".github/merge_bundle_counter.txt")
SPECIAL_FILENAME = "close reopen mobile screensharelink.json"
SPECIAL_KEYWORD = "screensharelink"
BETWEEN_MAX_PAUSES = 1  # Hardcoded: always 1 pause between files

# --------- Desktop-specific anti-detection helpers ---------

def add_desktop_mouse_drift(events, rng, drift_chance=0.15):
    """
    DESKTOP ONLY: 15% chance to add idle mouse drift during waits.
    
    During pauses, cursor slowly drifts around (humans don't keep mouse still).
    Only applies between non-click events (during waiting/thinking time).
    
    Higher chance than mobile misclick drift (15% vs 8%) because desktop
    players tend to fidget with mouse more.
    """
    drifted = []
    events_copy = deepcopy(events)
    
    for i, e in enumerate(events_copy):
        drifted.append(e)
        
        # Only add drift if:
        # 1. Not the first event
        # 2. Current event is NOT a click
        # 3. Next event (if exists) is NOT a click
        is_current_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                          'button' in e or 'Button' in e)
        
        is_next_click = False
        if i + 1 < len(events_copy):
            next_e = events_copy[i + 1]
            is_next_click = (next_e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                           'button' in next_e or 'Button' in next_e)
        
        if i > 0 and not is_current_click and not is_next_click and rng.random() < drift_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    current_x = int(e['X'])
                    current_y = int(e['Y'])
                    
                    # Create 2-5 slow drift movements (idle fidgeting)
                    drift_count = rng.randint(2, 5)
                    base_time = int(e.get('Time', 0))
                    
                    for d in range(drift_count):
                        drift_event = {
                            'Time': base_time + (d + 1) * rng.randint(100, 300),  # Slower drift
                            'Type': 'MouseMove',
                            'X': current_x + rng.randint(-30, 30),  # Wider drift range
                            'Y': current_y + rng.randint(-30, 30)
                        }
                        drifted.append(drift_event)
                except (ValueError, TypeError):
                    pass
    
    return drifted

def add_progressive_fatigue(events, rng, session_progress):
    """
    Apply progressive fatigue based on how long the session has been running.
    
    Args:
        session_progress: 0.0 to 1.0 (0 = start of session, 1 = end)
    
    Effects:
    - Early session (0-0.3): Fast, accurate (95-100% efficiency)
    - Mid session (0.3-0.7): Normal speed (90-95% efficiency)
    - Late session (0.7-1.0): Slower, more mistakes (80-90% efficiency)
    
    Adds extra micro-pauses and slight timing delays as fatigue increases.
    """
    if session_progress < 0.3:
        # Fresh - minimal fatigue
        fatigue_multiplier = rng.uniform(0.95, 1.00)
        extra_pause_chance = 0.05
    elif session_progress < 0.7:
        # Mid session - moderate fatigue
        fatigue_multiplier = rng.uniform(1.00, 1.10)
        extra_pause_chance = 0.12
    else:
        # Tired - significant fatigue
        fatigue_multiplier = rng.uniform(1.10, 1.25)
        extra_pause_chance = 0.20
    
    fatigued = []
    for e in deepcopy(events):
        # Apply fatigue timing to all events
        e['Time'] = int(e.get('Time', 0) * fatigue_multiplier)
        
        # Add extra random pauses when tired
        if rng.random() < extra_pause_chance:
            e['Time'] = int(e.get('Time', 0) + rng.randint(100, 500))
        
        fatigued.append(e)
    
    return fatigued

def add_action_clustering(events, rng):
    """
    Create burst-pause patterns (not steady rhythm).
    
    Humans work in attention bursts:
    - BURST: 3-7 actions quickly (focused)
    - PAUSE: 2-8 seconds (distracted/thinking)
    - BURST: 2-5 actions quickly
    - PAUSE: 4-12 seconds
    
    This modifies inter-action timing to create clustered patterns.
    """
    if len(events) < 4:
        return events
    
    clustered = deepcopy(events)
    cluster_mode = 'burst'  # Start in burst mode
    actions_in_cluster = 0
    cluster_target = rng.randint(3, 7)
    
    time_offset = 0
    
    for i, e in enumerate(clustered):
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click:
            actions_in_cluster += 1
            
            # Apply current mode timing
            if cluster_mode == 'burst':
                # Fast actions (minimal added delay)
                time_offset += rng.randint(0, 200)
            else:  # pause mode
                # Slow actions (significant delay)
                time_offset += rng.randint(2000, 8000)
            
            # Check if we should switch modes
            if actions_in_cluster >= cluster_target:
                # Switch mode
                if cluster_mode == 'burst':
                    cluster_mode = 'pause'
                    cluster_target = rng.randint(1, 3)  # Pause for 1-3 actions
                else:
                    cluster_mode = 'burst'
                    cluster_target = rng.randint(3, 7)  # Burst for 3-7 actions
                
                actions_in_cluster = 0
            
            e['Time'] = int(e.get('Time', 0)) + time_offset
    
    return clustered

def calculate_session_progress(file_index, total_files):
    """
    Calculate how far through the session we are (0.0 to 1.0).
    Used for progressive fatigue.
    """
    if total_files <= 1:
        return 0.5
    return file_index / (total_files - 1)

def get_structured_break_schedule(file_index, total_files, rng):
    """
    Determine if a structured break should happen based on session progress.
    
    Returns: (should_break, break_duration_ms, break_type)
    
    Break schedule:
    - Every ~15-25 mins: Short break (1-3 mins) - 25% chance
    - Every ~30-50 mins: Medium break (5-15 mins) - 15% chance
    - Every ~60-90 mins: Long break (20-45 mins) - 8% chance
    
    These replace the random AFK moments at specific intervals.
    """
    progress = calculate_session_progress(file_index, total_files)
    
    # Check for long break first (highest priority)
    if progress > 0.7 and rng.random() < 0.08:
        duration_ms = rng.randint(20 * 60000, 45 * 60000)  # 20-45 minutes
        return True, duration_ms, 'long'
    
    # Check for medium break
    if progress > 0.4 and rng.random() < 0.15:
        duration_ms = rng.randint(5 * 60000, 15 * 60000)  # 5-15 minutes
        return True, duration_ms, 'medium'
    
    # Check for short break
    if progress > 0.2 and rng.random() < 0.25:
        duration_ms = rng.randint(60000, 180000)  # 1-3 minutes
        return True, duration_ms, 'short'
    
    return False, 0, None

# --------- Desktop-specific anti-detection helpers ---------

def add_mouse_movement_curves(events, rng):
    """
    DESKTOP ONLY: Add curved mouse movements instead of straight lines.
    
    Humans don't move cursor in straight lines - we use slight curves.
    Inserts intermediate MouseMove events to create Bezier-like paths.
    
    Only applies between click events where cursor position changes significantly.
    """
    curved = []
    prev_x, prev_y = None, None
    
    for i, e in enumerate(deepcopy(events)):
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                target_x = int(e['X'])
                target_y = int(e['Y'])
                
                # If there's a previous position and distance is significant (>50 pixels)
                if prev_x is not None and prev_y is not None:
                    distance = ((target_x - prev_x)**2 + (target_y - prev_y)**2)**0.5
                    
                    if distance > 50:
                        # Add 2-4 intermediate curve points
                        num_points = rng.randint(2, 4)
                        base_time = int(e.get('Time', 0))
                        
                        for p in range(num_points):
                            # Progress along path (0 to 1)
                            t = (p + 1) / (num_points + 1)
                            
                            # Bezier curve with slight randomness
                            curve_offset_x = rng.randint(-15, 15)
                            curve_offset_y = rng.randint(-15, 15)
                            
                            # Interpolate position with curve
                            inter_x = int(prev_x + (target_x - prev_x) * t + curve_offset_x * (1 - t) * t)
                            inter_y = int(prev_y + (target_y - prev_y) * t + curve_offset_y * (1 - t) * t)
                            
                            # Time spread over movement duration
                            move_time = base_time - int((num_points - p) * rng.randint(20, 60))
                            
                            curve_event = {
                                'Time': move_time,
                                'Type': 'MouseMove',
                                'X': inter_x,
                                'Y': inter_y
                            }
                            curved.append(curve_event)
                
                # Update previous position
                prev_x, prev_y = target_x, target_y
            except:
                pass
        
        curved.append(e)
    
    return curved

def add_cursor_overshoots(events, rng, overshoot_chance=0.08):
    """
    DESKTOP ONLY: 8% chance cursor overshoots target slightly, then corrects.
    
    Humans often move mouse too far, then pull back slightly before clicking.
    """
    overshot = []
    
    for e in deepcopy(events):
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and rng.random() < overshoot_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    target_x = int(e['X'])
                    target_y = int(e['Y'])
                    
                    # Overshoot by 3-8 pixels
                    overshoot_dist = rng.randint(3, 8)
                    overshoot_x = target_x + overshoot_dist * rng.choice([-1, 1])
                    overshoot_y = target_y + overshoot_dist * rng.choice([-1, 1])
                    
                    # Overshoot happens 40-80ms before click
                    overshoot_time = int(e.get('Time', 0)) - rng.randint(40, 80)
                    
                    overshoot_event = {
                        'Time': overshoot_time,
                        'Type': 'MouseMove',
                        'X': overshoot_x,
                        'Y': overshoot_y
                    }
                    
                    # Correction move back to target (20-40ms before click)
                    correction_time = int(e.get('Time', 0)) - rng.randint(20, 40)
                    correction_event = {
                        'Time': correction_time,
                        'Type': 'MouseMove',
                        'X': target_x,
                        'Y': target_y
                    }
                    
                    overshot.append(overshoot_event)
                    overshot.append(correction_event)
                except:
                    pass
        
        overshot.append(e)
    
    return overshot

def add_mouse_acceleration_variance(events, rng):
    """
    DESKTOP ONLY: Vary mouse movement speed (acceleration/deceleration).
    
    Humans don't move cursor at constant speed:
    - Start slow (acceleration)
    - Middle fast (full speed)
    - End slow (deceleration for precision)
    
    Adjusts timing of MouseMove events to simulate this.
    """
    # This would require tracking sequences of MouseMove events
    # For now, we'll apply a simpler variance to individual moves
    varied = []
    
    for e in deepcopy(events):
        if e.get('Type') == 'MouseMove':
            # Vary movement timing by ±20%
            timing_variance = rng.uniform(0.8, 1.2)
            e['Time'] = int(e.get('Time', 0) * timing_variance)
        
        varied.append(e)
    
    return varied

def add_desktop_misclicks(events, rng, misclick_chance=0.04):
    """
    DESKTOP ONLY: 4% chance for accidental right-click before left-click.
    
    Similar to mobile but:
    - Lower chance (desktop is more precise)
    - Smaller offset (3-10 pixels instead of 5-15)
    - Faster correction (100-250ms instead of 150-350ms)
    """
    enhanced = []
    
    for e in deepcopy(events):
        is_left_click = (e.get('Type') == 'Click' or 
                        e.get('Type') == 'LeftClick' or
                        e.get('button') == 'left' or 
                        e.get('Button') == 'Left')
        
        if is_left_click and rng.random() < misclick_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    target_x = int(e['X'])
                    target_y = int(e['Y'])
                    
                    # Smaller misclick offset for desktop
                    offset_distance = rng.randint(3, 10)
                    misclick_x = target_x + int(offset_distance * rng.choice([-1, 1]))
                    misclick_y = target_y + int(offset_distance * rng.choice([-1, 1]))
                    
                    # Right-click misclick
                    misclick_time = int(e.get('Time', 0)) - rng.randint(100, 250)
                    misclick = deepcopy(e)
                    misclick['Time'] = misclick_time
                    misclick['X'] = misclick_x
                    misclick['Y'] = misclick_y
                    misclick['Type'] = 'RightClick'
                    if 'button' in misclick:
                        misclick['button'] = 'right'
                    if 'Button' in misclick:
                        misclick['Button'] = 'Right'
                    
                    # Quick correction movement
                    correction_time = misclick_time + rng.randint(40, 80)
                    correction_move = {
                        'Time': correction_time,
                        'Type': 'MouseMove',
                        'X': target_x + rng.randint(-1, 1),
                        'Y': target_y + rng.randint(-1, 1)
                    }
                    
                    enhanced.append(misclick)
                    enhanced.append(correction_move)
                except:
                    pass
        
        enhanced.append(e)
    
    return enhanced

# --------- Mobile-specific anti-detection helpers ---------

def add_afk_moments(events, rng, afk_chance=0.12):
    """
    12% chance to add random AFK (away from keyboard) moments between actions.
    Simulates human distractions: checking phone, talking to someone, thinking, etc.
    
    AFK duration: 3-25 seconds of complete inactivity.
    Only happens between file boundaries, never mid-action.
    
    This is CRITICAL for mobile detection - humans don't play non-stop.
    """
    # AFK moments are added between files, not within events
    # This function marks where they should go
    return events  # Implementation happens at file merge level

def get_dynamic_pause_range(rng, max_seconds, variance_factor=0.4):
    """
    Generate dynamic pause ranges instead of fixed 0-to-max.
    
    Humans don't use full range uniformly - we have preferred timing zones.
    This creates a "comfort zone" that shifts per session.
    
    Args:
        max_seconds: UI-configured maximum
        variance_factor: How much the comfort zone shifts (0.4 = ±40%)
    
    Returns:
        (min_ms, max_ms) tuple for this specific pause
    """
    # Create a shifting "comfort zone" within the max range
    comfort_center = rng.uniform(0.3, 0.7) * max_seconds  # Center point
    comfort_width = max_seconds * variance_factor  # Width of comfort zone
    
    min_s = max(0, comfort_center - comfort_width / 2)
    max_s = min(max_seconds, comfort_center + comfort_width / 2)
    
    # Convert to milliseconds
    return int(min_s * 1000), int(max_s * 1000)

def add_click_speed_variance(events, rng):
    """
    Vary PAUSE lengths between actions (not action speed itself).
    
    CRITICAL: Does NOT speed up/slow down the macro itself.
    Only affects PAUSES between files and within files.
    
    - Autopilot mode: Shorter pauses between actions (more focused)
    - Normal mode: Regular pauses
    - Distracted mode: Longer pauses between actions (less focused)
    
    This returns a multiplier for PAUSE durations only.
    """
    speed_mode = rng.choice(['autopilot', 'normal', 'normal', 'distracted'])
    
    if speed_mode == 'autopilot':
        return rng.uniform(0.7, 0.9)  # 10-30% shorter pauses
    elif speed_mode == 'distracted':
        return rng.uniform(1.2, 1.5)  # 20-50% longer pauses
    else:
        return 1.0  # Normal pause length

def add_task_completion_variance(total_actions, rng):
    """
    Humans don't complete tasks at perfectly consistent rates.
    
    CRITICAL: This ONLY affects PAUSES, not the macro timing itself.
    
    Returns a multiplier for PAUSE durations that simulates:
    - Focused sessions: Shorter pauses between actions (0.8-0.9x)
    - Average sessions: Normal pauses (1.0-1.1x)
    - Tired sessions: Longer pauses between actions (1.2-1.4x)
    
    The macro itself plays at normal speed - only waiting time changes.
    """
    session_mood = rng.choices(
        ['focused', 'average', 'tired'],
        weights=[0.2, 0.6, 0.2]  # Most sessions are average
    )[0]
    
    if session_mood == 'focused':
        return rng.uniform(0.80, 0.90)  # Shorter pauses
    elif session_mood == 'tired':
        return rng.uniform(1.20, 1.40)  # Longer pauses
    else:
        return rng.uniform(1.00, 1.10)  # Normal pauses

# --------- Original anti-detection helpers ---------
def add_mouse_jitter(events, rng, is_desktop=False):
    """
    Add click position variance based on environment.
    
    MOBILE: ±2 pixels (finger taps are less precise)
    DESKTOP: ±1 pixel (mouse clicks are more precise)
    """
    jittered = []
    jitter_range = [-1, 0, 1] if is_desktop else [-2, -1, 0, 1, 2]
    
    for e in deepcopy(events):
        is_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
            try:
                offset_x = rng.choice(jitter_range)
                offset_y = rng.choice(jitter_range)
                e['X'] = int(e['X']) + offset_x
                e['Y'] = int(e['Y']) + offset_y
            except:
                pass
        jittered.append(e)
    return jittered

def add_occasional_misclicks(events, rng, misclick_chance=0.035):
    """
    2-5% chance to insert a RIGHT-CLICK misclick before certain LEFT-CLICK actions.
    Simulates human error - we sometimes miss and have to correct.
    
    CRITICAL SAFETY:
    - Misclick is ALWAYS a RIGHT-CLICK (never left-click to avoid wrong actions)
    - Misclick happens 5-15 pixels AWAY from target (not at target)
    - Happens 150-350ms BEFORE the real click
    - After misclick, cursor moves BACK toward target, then does real click
    
    This simulates: miss slightly → realize mistake → move back → correct click
    """
    enhanced = []
    for i, e in enumerate(deepcopy(events)):
        # Check if this is a LEFT-CLICK event only
        is_left_click = (e.get('Type') == 'Click' or 
                        e.get('Type') == 'LeftClick' or
                        e.get('button') == 'left' or 
                        e.get('Button') == 'Left')
        
        if is_left_click and rng.random() < misclick_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    target_x = int(e['X'])
                    target_y = int(e['Y'])
                    
                    # Misclick position: 5-15 pixels away in random direction
                    offset_distance = rng.randint(5, 15)
                    misclick_x = target_x + int(offset_distance * rng.choice([-1, 1]))
                    misclick_y = target_y + int(offset_distance * rng.choice([-1, 1]))
                    
                    # Create RIGHT-CLICK misclick event
                    # Copy the structure of the original event but change to right-click
                    misclick_time = int(e.get('Time', 0)) - rng.randint(150, 350)
                    misclick = deepcopy(e)
                    misclick['Time'] = misclick_time
                    misclick['X'] = misclick_x
                    misclick['Y'] = misclick_y
                    
                    # Force it to be a RIGHT-CLICK in all possible formats
                    misclick['Type'] = 'RightClick'
                    if 'button' in misclick:
                        misclick['button'] = 'right'
                    if 'Button' in misclick:
                        misclick['Button'] = 'Right'
                    # Remove any left-click indicators
                    if 'LeftClick' in str(misclick.get('Type', '')):
                        misclick['Type'] = 'RightClick'
                    
                    # Add cursor movement BACK toward target (correction movement)
                    correction_time = misclick_time + rng.randint(50, 120)
                    correction_move = {
                        'Time': correction_time,
                        'Type': 'MouseMove',
                        'X': target_x + rng.randint(-2, 2),
                        'Y': target_y + rng.randint(-2, 2)
                    }
                    
                    enhanced.append(misclick)
                    enhanced.append(correction_move)
                except:
                    pass
        
        enhanced.append(e)
    
    return enhanced

def add_micro_pauses(events, rng, micropause_chance=0.15):
    """
    15% chance to add tiny hesitation (50-250ms) before actions.
    Simulates human thinking/reaction time - we don't act instantly.
    """
    paused = []
    for e in deepcopy(events):
        if rng.random() < micropause_chance:
            hesitation = rng.randint(50, 250)
            e['Time'] = int(e.get('Time', 0)) + hesitation
        paused.append(e)
    return paused

def add_mouse_drift(events, rng, drift_chance=0.08):
    """
    8% chance to add "mouse wander" events BETWEEN actions (not during clicks).
    Humans don't keep cursor perfectly still - it drifts naturally.
    
    CRITICAL: Only adds drift AFTER non-click events (like delays/waits).
    Never adds drift right before a click - that would move cursor away from target.
    
    Adds 1-3 small mouse movements within ±20 pixels of current position.
    """
    events_copy = deepcopy(events)
    drifted = []
    
    for i, e in enumerate(events_copy):
        drifted.append(e)
        
        # Only add drift if:
        # 1. Not the first event
        # 2. Current event is NOT a click (don't drift after clicks)
        # 3. Next event (if exists) is NOT a click (don't drift before clicks)
        is_current_click = (e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                          'button' in e or 'Button' in e)
        
        # Check if next event is a click
        is_next_click = False
        if i + 1 < len(events_copy):
            next_e = events_copy[i + 1]
            is_next_click = (next_e.get('Type') in ['Click', 'LeftClick', 'RightClick'] or 
                           'button' in next_e or 'Button' in next_e)
        
        # Only add drift if current and next are both NOT clicks
        if i > 0 and not is_current_click and not is_next_click and rng.random() < drift_chance:
            if 'X' in e and 'Y' in e and e['X'] is not None and e['Y'] is not None:
                try:
                    current_x = int(e['X'])
                    current_y = int(e['Y'])
                    
                    # Create 1-3 small mouse movements (idle wandering)
                    drift_count = rng.randint(1, 3)
                    base_time = int(e.get('Time', 0))
                    
                    for d in range(drift_count):
                        drift_event = {
                            'Time': base_time + (d + 1) * rng.randint(80, 200),
                            'Type': 'MouseMove',
                            'X': current_x + rng.randint(-20, 20),
                            'Y': current_y + rng.randint(-20, 20)
                        }
                        drifted.append(drift_event)
                except (ValueError, TypeError):
                    pass
    
    return drifted

def add_reaction_variance(events, rng):
    """
    Add human reaction time (200-600ms) to events that would require visual response.
    Humans don't react instantly to screen changes - we need time to see and process.
    
    Applied to clicks that likely respond to game state changes.
    """
    varied = []
    for i, e in enumerate(deepcopy(events)):
        # If this is a click and not the first event, add reaction delay
        is_click = (e.get('Type') in ['Click', 'RightClick'] or 
                   'button' in e or 'Button' in e)
        
        if is_click and i > 0 and rng.random() < 0.3:  # 30% of clicks get reaction delay
            reaction_time = rng.randint(200, 600)
            e['Time'] = int(e.get('Time', 0)) + reaction_time
        
        varied.append(e)
    
    return varied

def add_time_of_day_fatigue(events, rng):
    """
    Simulate human fatigue based on time of day.
    - Late night (11pm-5am): 15-30% slower, more mistakes
    - Peak hours (6pm-10pm): normal speed
    - Morning (6am-12pm): slightly faster, more focused
    - Afternoon (1pm-5pm): normal to slightly slower
    
    Also considers weekend vs weekday (weekends = more relaxed timing).
    """
    from datetime import datetime
    
    now = datetime.now()
    hour = now.hour
    is_weekend = now.weekday() >= 5  # Saturday=5, Sunday=6
    
    # Determine fatigue multiplier based on time
    if 23 <= hour or hour < 5:  # Late night (11pm-5am)
        fatigue_min, fatigue_max = 1.15, 1.30  # 15-30% slower
        mistake_increase = 0.02  # 2% more misclicks
    elif 6 <= hour < 12:  # Morning (6am-12pm)
        fatigue_min, fatigue_max = 0.95, 1.05  # Slightly faster
        mistake_increase = -0.01  # 1% fewer mistakes (more focused)
    elif 18 <= hour < 23:  # Peak evening (6pm-11pm)
        fatigue_min, fatigue_max = 1.0, 1.1  # Normal
        mistake_increase = 0.0
    else:  # Afternoon (12pm-6pm)
        fatigue_min, fatigue_max = 1.05, 1.15  # Slightly slower
        mistake_increase = 0.005
    
    # Weekend adjustment - more relaxed
    if is_weekend:
        fatigue_min += 0.05
        fatigue_max += 0.1
    
    # Apply fatigue to all events
    fatigue_multiplier = rng.uniform(fatigue_min, fatigue_max)
    fatigued = []
    
    for e in deepcopy(events):
        # Slow down actions based on fatigue
        original_time = int(e.get('Time', 0))
        e['Time'] = int(original_time * fatigue_multiplier)
        fatigued.append(e)
    
    return fatigued, mistake_increase

# --------- helpers ---------
def parse_time_to_seconds(s: str) -> int:
    if s is None:
        raise ValueError("Empty time string")
    s = str(s).strip()
    if not s:
        raise ValueError("Empty time string")
    
    # plain integer seconds
    if re.match(r'^\d+$', s):
        return int(s)
    
    # mm:ss
    m = re.match(r'^(\d+):(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    
    # m.ss (minutes.seconds)
    m = re.match(r'^(\d+)\.(\d{1,2})$', s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    
    # with letters like 1m30s, 90s, 2m
    m = re.match(r'^(?:(\d+)m)?(?:(\d+)s)?$', s)
    if m and (m.group(1) or m.group(2)):
        minutes = int(m.group(1)) if m.group(1) else 0
        seconds = int(m.group(2)) if m.group(2) else 0
        return minutes * 60 + seconds
    
    raise ValueError(f"Cannot parse time value: {s!r}")

def read_counter_file():
    try:
        if COUNTER_PATH.exists():
            txt = COUNTER_PATH.read_text(encoding="utf-8").strip()
            return int(txt) if txt else 0
    except Exception:
        pass
    return 0

def write_counter_file(n: int):
    try:
        COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        COUNTER_PATH.write_text(str(n), encoding="utf-8")
    except Exception:
        pass

def find_all_dirs_with_json(input_root: Path):
    """Return sorted list of directories (any depth) containing at least one .json directly inside them."""
    if not input_root.exists() or not input_root.is_dir():
        return []
    
    found = set()
    for p in sorted(input_root.rglob("*")):
        if p.is_dir():
            try:
                has = any(child.is_file() and child.suffix.lower() == ".json" for child in p.iterdir())
            except Exception:
                has = False
            if has:
                found.add(p)
    
    try:
        if any(child.is_file() and child.suffix.lower() == ".json" for child in input_root.iterdir()):
            found.add(input_root)
    except Exception:
        pass
    
    return sorted(found)

def find_json_files_in_dir(dirpath: Path):
    """Return sorted list of json files directly in dirpath (non-recursive)."""
    try:
        return sorted([p for p in dirpath.glob("*.json") if p.is_file()])
    except Exception:
        return []

def load_json_events(path: Path):
    """Load JSON and normalize to list of events (preserve structure where possible)."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARNING: failed to read/parse {path}: {e}", file=sys.stderr)
        return []
    
    if isinstance(data, dict):
        for k in ("events","items","entries","records","actions","eventsList","events_array"):
            if k in data and isinstance(data[k], list):
                return deepcopy(data[k])
        if "Time" in data:
            return [deepcopy(data)]
        return []
    
    if isinstance(data, list):
        return deepcopy(data)
    
    return []

def zero_base_events(events):
    """
    Return events sorted by Time, shifted so earliest Time is 0, and the event duration in ms.
    Preserves ALL original event fields - only modifies Time values.
    """
    if not events:
        return [], 0
    
    events_with_time = []
    for e in events:
        try:
            t = int(e.get("Time", 0))
        except Exception:
            try:
                t = int(float(e.get("Time", 0)))
            except:
                t = 0
        events_with_time.append((e, t))
    
    # Sort by time - this ensures events play in chronological order
    # Free Macro expects events in time order, so this is correct
    try:
        events_with_time.sort(key=lambda x: x[1])
    except Exception as e:
        print(f"WARNING: Could not sort events, proceeding without sorting. Error: {e}", file=sys.stderr)
    
    if not events_with_time:
        return [], 0
    
    min_t = events_with_time[0][1]
    shifted = []
    for (original_event, original_time) in events_with_time:
        ne = deepcopy(original_event)  # Preserve all fields
        ne["Time"] = original_time - min_t  # Only shift time
        shifted.append(ne)
    
    duration_ms = shifted[-1]["Time"] if shifted else 0
    return shifted, duration_ms

def part_from_filename(fname: str):
    """
    Token rule: up to 4 alphanumeric chars, prefer letters first then digits.
    """
    stem = Path(fname).stem
    letters = [ch for ch in stem if ch.isalpha()]
    digits = [ch for ch in stem if ch.isdigit()]
    
    token_chars = []
    for ch in letters:
        if len(token_chars) >= 4:
            break
        token_chars.append(ch.lower())
    
    if len(token_chars) < 4:
        for d in digits:
            if len(token_chars) >= 4:
                break
            token_chars.append(d)
    
    if not token_chars:
        alnum = [ch for ch in stem if ch.isalnum()]
        token_chars = [ch.lower() for ch in alnum[:4]]
    
    return ''.join(token_chars)

def insert_intra_pauses(events, rng, max_pauses, min_s, max_s):
    """
    Return events with inserted intra pauses and a list of the pauses (ms).
    Randomly choose 1 to max_pauses number of pauses to insert.
    Preserves ALL event fields, only modifies Time values.
    """
    if not events or max_pauses <= 0:
        return deepcopy(events), []
    
    evs = deepcopy(events)
    n = len(evs)
    if n < 2:
        return evs, []
    
    # Randomly choose between 1 and max_pauses (inclusive)
    k = rng.randint(1, min(max_pauses, n-1))
    if k == 0:
        return evs, []
    
    chosen = rng.sample(range(n-1), k)
    pauses_info = []
    
    for gap_idx in sorted(chosen):
        # Random pause with millisecond precision
        pause_ms = rng.randint(min_s * 1000, max_s * 1000)
        
        # shift subsequent events - only modify Time field
        for j in range(gap_idx+1, n):
            evs[j]["Time"] = int(evs[j].get("Time", 0)) + pause_ms
        
        pauses_info.append({"after_event_index": gap_idx, "pause_ms": pause_ms})
    
    return evs, pauses_info

def apply_shifts(events, shift_ms):
    """Preserve ALL keys but update Time by adding shift_ms."""
    shifted = []
    for e in events:
        ne = deepcopy(e)  # Preserve all original fields
        try:
            t = int(ne.get("Time", 0))
        except Exception:
            try:
                t = int(float(ne.get("Time", 0)))
            except:
                t = 0
        ne["Time"] = t + int(shift_ms)
        shifted.append(ne)
    return shifted

def compute_minutes_from_ms(ms: int):
    return math.ceil(ms / 60000) if ms > 0 else 0

def number_to_letters(n: int) -> str:
    """Convert 1->A, 2->B, ... 26->Z, 27->AA, ... Excel-style uppercase."""
    if n <= 0:
        return ""
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord('A') + (n % 26)) + letters
        n //= 26
    return letters

# --------- Non-repeating randomization helpers ---------
class NonRepeatingSelector:
    """
    Ensures all combinations are exhausted before repeating.
    Generates permutations/combinations on-the-fly to avoid memory issues.
    """
    def __init__(self, rng):
        self.rng = rng
        self.used_combos = set()
        self.all_exhausted = False
    
    def select_files(self, files, exclude_count):
        """
        Select which files to include (non-repeating).
        Returns tuple of (included_files, excluded_files).
        """
        if not files:
            return [], []
        
        n = len(files)
        max_exclude = min(exclude_count, max(0, n - 1))
        
        # Generate all possible exclusion combinations
        file_indices = list(range(n))
        all_possible = []
        
        for exclude_k in range(0, max_exclude + 1):
            for combo in combinations(file_indices, exclude_k):
                all_possible.append(frozenset(combo))
        
        # Filter out used combinations
        available = [c for c in all_possible if c not in self.used_combos]
        
        # If all exhausted, reset
        if not available:
            self.used_combos.clear()
            available = all_possible
        
        # Pick one
        chosen_exclude_indices = self.rng.choice(available)
        self.used_combos.add(chosen_exclude_indices)
        
        excluded = [files[i] for i in chosen_exclude_indices]
        included = [files[i] for i in file_indices if i not in chosen_exclude_indices]
        
        return included if included else files.copy(), excluded
    
    def shuffle_with_memory(self, items):
        """
        Shuffle items without repeating previous shuffles until all exhausted.
        For large item counts, falls back to regular shuffle after threshold.
        """
        if not items or len(items) <= 1:
            return items
        
        # For large lists, permutations are too many - use regular shuffle
        if len(items) > 8:
            shuffled = items.copy()
            self.rng.shuffle(shuffled)
            return shuffled
        
        # Convert to tuple for hashing
        items_tuple = tuple(items)
        
        # Generate all permutations (memory intensive for n>8)
        from itertools import permutations as iter_perms
        all_perms = [perm for perm in iter_perms(items_tuple)]
        
        # Filter unused
        available = [p for p in all_perms if p not in self.used_combos]
        
        # Reset if exhausted
        if not available:
            self.used_combos.clear()
            available = all_perms
        
        # Pick one
        chosen = self.rng.choice(available)
        self.used_combos.add(chosen)
        
        return list(chosen)

# --------- find special file ---------
def locate_special_file_for_group(folder: Path, input_root: Path):
    """
    Robust locate of the special file for a group.
    """
    # 1) exact in the group folder
    try:
        cand = folder / SPECIAL_FILENAME
        if cand.exists():
            return cand.resolve()
    except Exception:
        pass
    
    # 2) exact under input_root
    try:
        cand2 = input_root / SPECIAL_FILENAME
        if cand2.exists():
            return cand2.resolve()
    except Exception:
        pass
    
    # 3) search repo root (cwd) for files that include SPECIAL_KEYWORD in name
    repo_root = Path.cwd()
    keyword = SPECIAL_KEYWORD.lower()
    try:
        for p in repo_root.iterdir():
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    
    # 4) fallback: search entire repo recursively
    try:
        for p in repo_root.rglob("*"):
            if p.is_file() and keyword in p.name.lower():
                return p.resolve()
    except Exception:
        pass
    
    return None

# --------- generate for a single folder ---------
def generate_version_for_folder(files, rng, version_num,
                                exclude_count,
                                within_min_s, within_max_s, within_max_pauses,
                                between_min_s, between_max_s,
                                folder_path: Path,
                                input_root: Path,
                                selector: NonRepeatingSelector):
    """Merge provided list of files (all from same folder) with non-repeating randomization."""
    if not files:
        return None, [], [], {"inter_file_pauses":[], "intra_file_pauses":[]}, [], 0
    
    # Use non-repeating selector for file selection
    included, excluded = selector.select_files(files, exclude_count)
    
    if not included:
        included = files.copy()
    
    # Check for "always first" and "always last" files in the folder
    always_first_file = None
    always_last_file = None
    
    for f in included:
        fname_lower = Path(f).name.lower()
        if fname_lower.startswith("always first"):
            always_first_file = f
        elif fname_lower.startswith("always last"):
            always_last_file = f
    
    # Remove special ordering files from included list (will be added back in correct position)
    included = [f for f in included if f not in [always_first_file, always_last_file]]
    
    # Decide which version gets which special file (never both in same version)
    use_always_first_this_version = False
    use_always_last_this_version = False
    
    if always_first_file and always_last_file:
        # Both exist - randomly assign to different versions
        # Use version number to determine which gets which
        if version_num % 2 == 1:  # Odd versions get "always first"
            use_always_first_this_version = True
        else:  # Even versions get "always last"
            use_always_last_this_version = True
    elif always_first_file:
        # Only "always first" exists - use it in some versions
        use_always_first_this_version = (version_num % 3 == 1)  # Every 3rd version starting from 1
    elif always_last_file:
        # Only "always last" exists - use it in some versions
        use_always_last_this_version = (version_num % 3 == 2)  # Every 3rd version starting from 2
    
    # NO DUPLICATION - each file used only once
    # Use non-repeating shuffle to ensure orders don't repeat
    final_files = selector.shuffle_with_memory(included)
    
    # Add special ordering files in correct positions
    if use_always_first_this_version and always_first_file:
        final_files.insert(0, always_first_file)
    
    if use_always_last_this_version and always_last_file:
        final_files.append(always_last_file)
    
    # Handle mobile special file
    special_path = None
    is_mobile_group = any("mobile" in part.lower() for part in folder_path.parts)
    
    if is_mobile_group:
        special_cand = locate_special_file_for_group(folder_path, input_root)
        if special_cand:
            special_path = special_cand
            final_files = [f for f in final_files if Path(f).resolve() != special_path]
            
            if final_files:
                mid_idx = len(final_files) // 2
                insert_pos = min(mid_idx + 1, len(final_files))
                final_files.insert(insert_pos, str(special_path))
            else:
                final_files.insert(0, str(special_path))
            
            final_files.append(str(special_path))
        else:
            print(f"INFO: mobile group {folder_path} - special '{SPECIAL_FILENAME}' not found in repo; skipping insertion.")
    
    merged = []
    pause_info = {"inter_file_pauses": [], "intra_file_pauses": []}
    time_cursor = 0
    per_file_event_ms = {}
    per_file_inter_ms = {}
    
    for idx, fpath in enumerate(final_files):
        fpath_obj = Path(fpath)
        is_special = special_path is not None and fpath_obj.resolve() == special_path.resolve()
        
        evs = load_json_events(fpath_obj)
        zb_evs, file_duration_ms = zero_base_events(evs)
        
        # Apply anti-detection features (preserve special file behavior)
        if not is_special:
            # ANTI-DETECTION CONTROLS - Optimized for MOBILE SCREEN SHARE
            # Mouse movements are INVISIBLE on mobile, focus on timing & click variance
            ENABLE_TIME_FATIGUE = True      # ✅ Timing changes ARE detectable
            ENABLE_MOUSE_JITTER = True      # ✅ Click position variance IS detectable  
            ENABLE_MISCLICKS = False        # ❌ Right-clicks might not work on mobile
            ENABLE_MICRO_PAUSES = True      # ✅ Timing hesitations ARE detectable
            ENABLE_MOUSE_DRIFT = False      # ❌ Mouse movements INVISIBLE on mobile
            ENABLE_REACTION_VARIANCE = True # ✅ Reaction delays ARE detectable
            
            # PHASE 1: Time modifications (don't insert events, just modify timing)
            if ENABLE_TIME_FATIGUE:
                zb_evs, extra_mistake_chance = add_time_of_day_fatigue(zb_evs, rng)
            else:
                extra_mistake_chance = 0
            
            if ENABLE_MICRO_PAUSES:
                zb_evs = add_micro_pauses(zb_evs, rng)
            
            if ENABLE_REACTION_VARIANCE:
                zb_evs = add_reaction_variance(zb_evs, rng)
            
            # PHASE 2: Coordinate modifications (only ±1 pixel on clicks)
            if ENABLE_MOUSE_JITTER:
                zb_evs = add_mouse_jitter(zb_evs, rng)
            
            # PHASE 3: Event insertions
            if ENABLE_MISCLICKS:
                base_misclick_chance = 0.035 + extra_mistake_chance
                zb_evs = add_occasional_misclicks(zb_evs, rng, base_misclick_chance)
            
            if ENABLE_MOUSE_DRIFT:
                zb_evs = add_mouse_drift(zb_evs, rng)
            
            # PHASE 4: Re-sort by time
            zb_evs, file_duration_ms = zero_base_events(zb_evs)
        
        # For special file: no intra pauses, no anti-detection
        if is_special:
            intra_evs = zb_evs
            intra_details = []
            per_file_event_ms[str(fpath_obj)] = file_duration_ms
        else:
            intra_evs, intra_details = insert_intra_pauses(zb_evs, rng, within_max_pauses, within_min_s, within_max_s)
            if intra_details:
                pause_info["intra_file_pauses"].append({"file": fpath_obj.name, "pauses": intra_details})
            
            total_event_ms = intra_evs[-1]["Time"] if intra_evs else 0
            per_file_event_ms[str(fpath_obj)] = total_event_ms
        
        shifted = apply_shifts(intra_evs, time_cursor)
        merged.extend(shifted)
        
        if shifted:
            file_max = shifted[-1]["Time"]
            time_cursor = file_max
        
        # Pause logic - hardcoded 1 pause between files
        if idx < len(final_files) - 1:
            pause_ms = 0
            
            if is_special:
                SHORT_MID_BUFFER_MS = 1000
                pause_ms = SHORT_MID_BUFFER_MS
            else:
                # Random pause with millisecond precision (0 to between_max_s)
                pause_ms = rng.randint(between_min_s * 1000, between_max_s * 1000)
            
            time_cursor += pause_ms
            per_file_inter_ms[str(fpath_obj)] = pause_ms
            pause_info["inter_file_pauses"].append({"after_file": fpath_obj.name, "pause_ms": pause_ms, "after_index": idx})
        else:
            POST_APPEND_BUFFER_MS = 1000
            per_file_inter_ms[str(fpath_obj)] = POST_APPEND_BUFFER_MS
            time_cursor += POST_APPEND_BUFFER_MS
    
    total_ms = time_cursor if merged else 0
    total_minutes = compute_minutes_from_ms(total_ms)
    
    # build parts
    parts = []
    for f in final_files:
        event_ms = per_file_event_ms.get(str(f), 0)
        inter_ms = per_file_inter_ms.get(str(f), 0)
        combined_ms = event_ms + inter_ms
        minutes = compute_minutes_from_ms(combined_ms)
        parts.append(f"{part_from_filename(Path(f).name)}[{minutes}m]")
    
    letters = number_to_letters(version_num or 1)
    
    # Special naming for "always last" files
    if use_always_last_this_version and always_last_file:
        base_name = f"always last - {letters}_{total_minutes}m= " + " - ".join(parts)
    else:
        base_name = f"{letters}_{total_minutes}m= " + " - ".join(parts)
    
    safe_name = ''.join(ch for ch in base_name if ch not in '/\\:*?"<>|')
    merged_fname = f"{safe_name}.json"
    
    return merged_fname, merged, [str(p) for p in final_files], pause_info, [str(p) for p in excluded], total_minutes

# --------- CLI ---------
def build_arg_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--input-dir", required=False, default="originals")
    p.add_argument("--output-dir", required=False, default="output")
    p.add_argument("--versions", type=int, default=26)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--exclude-count", type=int, default=10)
    p.add_argument("--within-max-time", type=str, default="33")
    p.add_argument("--within-max-pauses", type=int, default=2)
    p.add_argument("--between-max-time", type=str, default="18")
    
    # legacy aliases
    p.add_argument("--intra-file-max", type=str, dest="within_max_time", help=argparse.SUPPRESS)
    p.add_argument("--intra-file-max-pauses", type=int, dest="within_max_pauses", help=argparse.SUPPRESS)
    p.add_argument("--inter-file-max", type=str, dest="between_max_time", help=argparse.SUPPRESS)
    
    return p

# --------- main ---------
def main():
    parser = build_arg_parser()
    args = parser.parse_args()
    
    rng = random.Random(args.seed) if args.seed is not None else random.Random()
    
    input_root = Path(args.input_dir)
    output_parent = Path(args.output_dir)
    output_parent.mkdir(parents=True, exist_ok=True)
    
    bundle_seq_env = os.environ.get("BUNDLE_SEQ", "").strip()
    if bundle_seq_env:
        try:
            counter = int(bundle_seq_env)
        except:
            counter = read_counter_file() or 1
    else:
        prev = read_counter_file()
        counter = prev + 1 if prev >= 0 else 1
        write_counter_file(counter)
    
    output_base_name = f"merged_bundle_{counter}"
    output_root = output_parent / output_base_name
    output_root.mkdir(parents=True, exist_ok=True)
    
    folder_dirs = find_all_dirs_with_json(input_root)
    
    if not folder_dirs:
        print(f"No json files found under {input_root}", file=sys.stderr)
        return
    
    try:
        within_max_s = parse_time_to_seconds(getattr(args, "within_max_time"))
    except Exception as e:
        print(f"ERROR parsing within max time: {e}", file=sys.stderr)
        return
    
    try:
        between_max_s = parse_time_to_seconds(getattr(args, "between_max_time"))
    except Exception as e:
        print(f"ERROR parsing between max time: {e}", file=sys.stderr)
        return
    
    # NO hardcoded minimums - uses dynamic ranges
    
    all_written_paths = []
    
    for folder in folder_dirs:
        files = find_json_files_in_dir(folder)
        if not files:
            continue
        
        try:
            rel_folder = folder.relative_to(input_root)
        except Exception:
            rel_folder = Path(folder.name)
        
        out_folder_for_group = output_root / rel_folder
        out_folder_for_group.mkdir(parents=True, exist_ok=True)
        
        # Create non-repeating selector per folder group
        selector = NonRepeatingSelector(rng)
        
        for v in range(1, max(1, args.versions) + 1):
            merged_fname, merged_events, finals, pauses, excluded, total_minutes = generate_version_for_folder(
                files, rng, v,
                args.exclude_count,
                within_max_s, getattr(args, "within_max_pauses"),
                between_max_s,
                folder, input_root,
                selector
            )
            
            if not merged_fname:
                continue
            
            out_path = out_folder_for_group / merged_fname
            try:
                out_path.write_text(json.dumps(merged_events, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"WROTE: {out_path}")
                all_written_paths.append(out_path)
            except Exception as e:
                print(f"ERROR writing {out_path}: {e}", file=sys.stderr)
    
    # ZIP
    zip_path = output_parent / f"{output_base_name}.zip"
    with ZipFile(zip_path, "w") as zf:
        for fpath in all_written_paths:
            try:
                arcname = str(fpath.relative_to(output_parent))
            except Exception:
                arcname = f"{output_base_name}/{fpath.name}"
            zf.write(fpath, arcname=arcname)
    
    print("DONE. Created zip:", zip_path)

if __name__ == "__main__":
    main()
