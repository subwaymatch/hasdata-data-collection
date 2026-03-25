-- Migration: add missing columns to zillow_properties
-- Safe to re-run (ADD COLUMN IF NOT EXISTS).
-- Existing rows will have NULL for all new columns.

-- Top-level fields
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS available_from TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS building_id BIGINT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS contingent_listing_type TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS fees JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS lease_term TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS open_house_schedule JSONB;

-- resoData scalar fields
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_above_grade_finished_area TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_additional_parcels_description TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_association_fee TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_availability_date TIMESTAMPTZ;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_bathrooms_one_quarter INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_bathrooms_three_quarter INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_builder_model TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_building_area_source TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_carport_parking_capacity INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_common_walls TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_cumulative_days_on_market TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_fencing TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_has_association BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_has_carport BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_has_pets_allowed BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_has_waterfront_view BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_hoa_fee TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_hoa_fee_total TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_irrigation_water_rights_yn BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_lease_term TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_levels TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_list_aor TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_living_area_range_units TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_main_level_bathrooms INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_main_level_bedrooms INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_property_condition TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_stories_total INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_structure_type TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_virtual_tour TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_water_view TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_water_view_yn BOOLEAN;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_year_built INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_year_built_effective INTEGER;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_zoning TEXT;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_zoning_description TEXT;

-- resoData container (JSONB) fields
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_associations JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_door_features JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_green_building_verification_type JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_green_energy_efficient JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_green_energy_generation JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_green_water_conservation JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_horse_amenities JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_media JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_other_equipment JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_other_facts JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_other_parking JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_other_structures JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_pool_features JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_road_surface_type JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_spa_features JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_utilities JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_view JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_waterfront_features JSONB;
ALTER TABLE zillow_properties ADD COLUMN IF NOT EXISTS reso_window_features JSONB;
