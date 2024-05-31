-- Funkcija za pronalazak vozila u blizini
DROP FUNCTION IF EXISTS find_nearby_vehicles(geometry, double precision);

-- Funkcija za pronalazak najbliže ceste
DROP FUNCTION IF EXISTS get_nearest_road(geometry);

-- Funkcija za određivanje strane ceste na kojoj se vozilo nalazi
DROP FUNCTION IF EXISTS get_side_of_road(geometry, geometry);

-- Funkcije za obavještavanje
DROP FUNCTION IF EXISTS notify_wrong_direction(bigint, time, text, geometry);
DROP FUNCTION IF EXISTS notify_risk_of_crash(bigint, bigint, time, geometry);

-- Okidač
DROP TRIGGER IF EXISTS vehicle_direction_trigger ON vehicle_gps_datas;

-- Glavna funkcija
DROP FUNCTION IF EXISTS process_vehicle_direction();

----------------------------------------------------

-- Određivanje vozila u blizini
CREATE OR REPLACE FUNCTION find_nearby_vehicles(vehicle_location geometry, search_radius double precision)
RETURNS TABLE(id_vehicle bigint, location geometry) AS $$
BEGIN
RETURN QUERY
    WITH latest_locations AS (
    SELECT DISTINCT ON (vehicle_id) vehicle_id::bigint AS vehicle_id, location_geometry AS loc
    FROM vehicle_gps_datas
    ORDER BY vehicle_id, time DESC
  )
SELECT vehicle_id, loc
FROM latest_locations
WHERE ST_DWithin(loc, vehicle_location, search_radius);
END;
$$ LANGUAGE plpgsql;

-- Obavještavanje o krivom smjeru
CREATE OR REPLACE FUNCTION notify_wrong_direction(
    vehicle_id bigint,
    moment time,
    msg text,
    vehicle_location geometry)
RETURNS void AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration DOUBLE PRECISION;
    nearby_vehicles RECORD;
    vehicles_in_radius TEXT := '';
    vehicle_ids TEXT[] := '{}';
BEGIN
    start_time := clock_timestamp();

    -- Pronađi vozila unutar radijusa od 500m
FOR nearby_vehicles IN
SELECT DISTINCT v.id_vehicle
FROM find_nearby_vehicles(vehicle_location, 500.00) AS v -- u metrima
    LOOP
        IF nearby_vehicles.id_vehicle::text != vehicle_id::text AND nearby_vehicles.id_vehicle::text != ALL(vehicle_ids) THEN
            vehicles_in_radius := vehicles_in_radius || nearby_vehicles.id_vehicle || ', ';
vehicle_ids := array_append(vehicle_ids, nearby_vehicles.id_vehicle::text);
END IF;
END LOOP;

    -- Formatiraj poruku
    IF length(vehicles_in_radius) > 0 THEN
        vehicles_in_radius := substring(vehicles_in_radius FROM 1 FOR length(vehicles_in_radius) - 2);
END IF;

    -- Dodaj listu vozila u poruku
    IF vehicles_in_radius <> '' THEN
        msg := msg || '. [NEARBY VEHICLES]: ' || vehicles_in_radius;
ELSE
        msg := msg || '. [NEARBY VEHICLES]: NONE';
END IF;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    PERFORM pg_notify('testing_channel', '[TESTING-WD] Notification and participant determination took: ' || duration || ' seconds.');

    -- Pošalji obavijest
    PERFORM pg_notify('wrong_direction_channel', msg);

END;
$$ LANGUAGE plpgsql;

-- Obavještavanje o riziku od sudara
CREATE OR REPLACE FUNCTION notify_risk_of_crash(
    vehicle_id bigint,
    nearby_vehicle_id bigint,
    moment time,
    vehicle_location geometry)
RETURNS void AS $$
DECLARE
start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration DOUBLE PRECISION;
    nearby_vehicles RECORD;
    vehicles_in_radius TEXT := '';
    vehicle_ids TEXT[] := '{}';
BEGIN
    start_time := clock_timestamp();

    -- Pronađi vozila unutar radijusa od 200m
FOR nearby_vehicles IN
SELECT DISTINCT v.id_vehicle
FROM find_nearby_vehicles(vehicle_location, 200.00) AS v -- in meters
    LOOP
        IF nearby_vehicles.id_vehicle::text != vehicle_id::text AND nearby_vehicles.id_vehicle::text != ALL(vehicle_ids) THEN
            vehicles_in_radius := vehicles_in_radius || nearby_vehicles.id_vehicle || ', ';
vehicle_ids := array_append(vehicle_ids, nearby_vehicles.id_vehicle::text);
END IF;
END LOOP;

    -- Formatiraj poruku
    IF length(vehicles_in_radius) > 0 THEN
        vehicles_in_radius := substring(vehicles_in_radius FROM 1 FOR length(vehicles_in_radius) - 2);
END IF;

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    PERFORM pg_notify('testing_channel', '[TESTING-RC] Notification and participant determination took: ' || duration || ' seconds.');

    -- Pošalji obavijest
    IF vehicles_in_radius <> '' THEN
        PERFORM pg_notify('risk_of_crash_channel', '[POTENTIAL CRASH]: Vehicle ' || vehicle_id || ' is within 5 meters of vehicle ' || nearby_vehicle_id || ' at ' || moment || '. [NEARBY VEHICLES]: ' || vehicles_in_radius);
ELSE
        PERFORM pg_notify('risk_of_crash_channel', '[POTENTIAL CRASH]: Vehicle ' || vehicle_id || ' is within 5 meters of vehicle ' || nearby_vehicle_id || ' at ' || moment || '. [NEARBY VEHICLES]: NONE');
END IF;

END;
$$ LANGUAGE plpgsql;

-- Pronalazak najbliže ceste
CREATE OR REPLACE FUNCTION get_nearest_road(vehicle_point geometry)
RETURNS TABLE(road_segment_id bigint, distance double precision, road_way geometry) AS $$
BEGIN
RETURN QUERY
SELECT osm_id, ST_Distance(vehicle_point, way) as distance, way as road_way
FROM planet_osm_roads
WHERE highway IS NOT NULL
ORDER BY vehicle_point <-> way
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Pronalazak strane ceste na kojoj se vozilo nalazi
CREATE OR REPLACE FUNCTION get_side_of_road(road_line geometry, vehicle_point geometry)
RETURNS TEXT AS $$
DECLARE
road_start geometry;
    road_end geometry;
    point_on_road geometry;
    side TEXT;
    cross_product float8;
	start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration DOUBLE PRECISION;
BEGIN
    start_time := clock_timestamp();

    -- Pronađi početnu i krajnju točku ceste
    road_start := ST_StartPoint(road_line);
    road_end := ST_EndPoint(road_line);

    -- Pronađi točku na cesti koja je najbliža vozilu
    point_on_road := ST_ClosestPoint(road_line, vehicle_point);

    -- Izračunaj vektorski produkt
    cross_product := (ST_X(point_on_road) - ST_X(road_start)) * (ST_Y(vehicle_point) - ST_Y(road_start)) -
                     (ST_Y(point_on_road) - ST_Y(road_start)) * (ST_X(vehicle_point) - ST_X(road_start));

    -- Odredi stranu ceste
    IF cross_product > 0 THEN
        side := 'left';
ELSE
        side := 'right';
END IF;

	end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    PERFORM pg_notify('testing_channel', '[TESTING-TW] Determining on which side of the road vehicle is took: ' || duration || ' seconds.');

RETURN side;
END;
$$ LANGUAGE plpgsql;



-- Glavna funkcija
CREATE OR REPLACE FUNCTION process_vehicle_direction()
RETURNS TRIGGER AS $$
DECLARE
nearby_vehicle RECORD;
    previous_location GEOMETRY;
    road_direction REAL;
    vehicle_direction REAL;
    nearest_road RECORD;
    nearby_nearest_road RECORD;
    oneway_from_table BOOLEAN;
    msg TEXT;
    road_segment_geometry GEOMETRY;
    side_of_road TEXT;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration DOUBLE PRECISION;
BEGIN
    -- Odredi prethodnu lokaciju vozila
SELECT location_geometry INTO previous_location
FROM vehicle_gps_datas
WHERE vehicle_id = NEW.vehicle_id
ORDER BY time DESC
    LIMIT 1 OFFSET 1;

-- Ako je ovo prva lokacija vozila, preskoči provjeru
IF previous_location IS NULL THEN
        RETURN NEW;
END IF;

    -- Ako su oba vozila preblizu
SELECT vehicle_id, time, location_geometry INTO nearby_vehicle
FROM vehicle_gps_datas
WHERE vehicle_id != NEW.vehicle_id
  AND ST_Distance(NEW.location_geometry, location_geometry) < 5
  AND ABS(EXTRACT(EPOCH FROM (NEW.time - time))) <= 2;

-- Pronađi najbližu cestu
SELECT * INTO nearest_road FROM get_nearest_road(NEW.location_geometry);
IF nearest_road IS NULL THEN
        RETURN NEW;
END IF;

    -- Ako su oba vozila na istoj cesti, obavijesti o mogućem sudaru
    IF nearby_vehicle.vehicle_id IS NOT NULL THEN
SELECT * INTO nearby_nearest_road FROM get_nearest_road(nearby_vehicle.location_geometry);
IF nearby_nearest_road.road_segment_id = nearest_road.road_segment_id THEN
            PERFORM notify_risk_of_crash(NEW.vehicle_id, nearby_vehicle.vehicle_id, nearby_vehicle.time, NEW.location_geometry);
END IF;
END IF;

    start_time := clock_timestamp();

    -- Odredi da li se vozilo kreće u ispravnom smjeru
SELECT ST_Azimuth(ST_StartPoint(way), ST_EndPoint(way)), oneway INTO road_direction, oneway_from_table
FROM planet_osm_roads
WHERE osm_id = nearest_road.road_segment_id;
vehicle_direction := ST_Azimuth(previous_location, NEW.location_geometry);

    end_time := clock_timestamp();
    duration := EXTRACT(EPOCH FROM (end_time - start_time));
    PERFORM pg_notify('testing_channel', '[TESTING]: Processing (one-way) direction for vehicle ' || NEW.vehicle_id || ' took ' || duration || ' seconds.');

SELECT way INTO road_segment_geometry
FROM planet_osm_roads
WHERE osm_id = nearest_road.road_segment_id;

IF oneway_from_table IS NULL OR oneway_from_table = 'no' THEN
        start_time := clock_timestamp();
        side_of_road := get_side_of_road(road_segment_geometry, NEW.location_geometry);
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));
        PERFORM pg_notify('testing_channel', '[TESTING]: Processing (two-way) direction for vehicle ' || NEW.vehicle_id || ' took ' || duration || ' seconds.');

		side_of_road := get_side_of_road(road_segment_geometry, NEW.location_geometry);

SELECT INTO msg CASE
            WHEN side_of_road = 'right' THEN
                '' -- Vozilo je u točnom smjeru
            ELSE
                CASE
                    WHEN ABS(vehicle_direction - road_direction) < PI() / 4 THEN -- Provjera smjera
                        '[WRONG-WAY]: Vehicle ' || NEW.vehicle_id || ' is in the wrong (two-way) direction at ' || NEW.time
                    ELSE
                        '' -- Vozilo je u točnom smjeru
END
END;
ELSE
SELECT INTO msg CASE
				WHEN ABS(vehicle_direction - road_direction) < PI() / 2 THEN
					'' -- Vozilo je u točnom smjeru
				ELSE
					'[WRONG-WAY]: Vehicle ' || NEW.vehicle_id || ' is in the wrong direction at ' || NEW.time
END;
END IF;

    IF msg <> '' THEN
        PERFORM notify_wrong_direction(NEW.vehicle_id, NEW.time, msg, NEW.location_geometry);
END IF;

RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Okidač
CREATE TRIGGER vehicle_direction_trigger
    AFTER INSERT ON vehicle_gps_datas
    FOR EACH ROW
    EXECUTE FUNCTION process_vehicle_direction();
