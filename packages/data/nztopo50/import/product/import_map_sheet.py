import os
import uuid
from numbers import Real

import geopandas as gpd
import pandas as pd
from sqlalchemy import Date, Integer, String, create_engine, text
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
from sqlalchemy.dialects.postgresql import UUID as PG_UUID


class MapSheetImporter:
    def __init__(
        self,
        db_params,
        source,
        path,
        file_name,
        layer=None,
        carto_schema="carto",
        release_schema="release66",
        topo_id_name="id",
        final_drop_fields=False,
    ):
        """
        Initialize MapSheetImporter with database and data source configuration.
        
        Args:
            db_params: SQLAlchemy database connection string
            source: Source type ('windows-gpkg', 'windows-shp', 'aws-gpkg')
            path: File system path to data source
            file_name: Name of the source file
            layer: Layer name for GeoPackage sources (optional)
            carto_schema: PostgreSQL schema for carto tables (default: 'carto')
            release_schema: PostgreSQL schema for release tables (default: 'release66')
            topo_id_name: Column name for topo IDs (default: 'id')
            final_drop_fields: Whether to drop post-update columns (default: False)
        """
        self.db_params = db_params
        self.source = source
        self.path = path
        self.file_name = file_name
        self.layer = layer
        self.carto_schema = carto_schema
        self.release_schema = release_schema
        self.topo_id_name = topo_id_name
        self.final_drop_fields = final_drop_fields

        self.schema = "carto"
        self.table_name = "nztopo50_map_sheet"
        self.dtype_mapping = {
            "id": PG_UUID(as_uuid=True),
            "t50_fid": Integer(),
            "type": String(50),
            "example_name": String(150),
            "example_class": String(30),
            "sheet_code": String(21),
            "sheet_name": String(50),
            "edition": String(30),
            "origin_x": DOUBLE_PRECISION(),
            "origin_y": DOUBLE_PRECISION(),
            "example_point_id": PG_UUID(as_uuid=True),
            "published_version": String(25),
            "published_at": Date(),
            "updated_at": Date(),
        }

    @staticmethod
    def top_left_from_geometry(geom):
        """
        Extract top-left corner coordinates from a geometry object.
        
        Returns the minimum x-coordinate and maximum y-coordinate from the geometry,
        representing the top-left corner of the bounding box.
        
        Args:
            geom: A geometry object with __geo_interface__ property
            
        Returns:
            Tuple of (x, y) coordinates or (pd.NA, pd.NA) if geometry is empty/None
        """
        if geom is None or geom.is_empty:
            return (pd.NA, pd.NA)

        xs = []
        ys = []

        def walk_coords(node):
            if isinstance(node, (list, tuple)):
                if len(node) >= 2 and isinstance(node[0], Real) and isinstance(node[1], Real):
                    xs.append(float(node[0]))
                    ys.append(float(node[1]))
                else:
                    for child in node:
                        walk_coords(child)

        walk_coords(geom.__geo_interface__.get("coordinates"))
        if not xs or not ys:
            return (pd.NA, pd.NA)

        return (min(xs), max(ys))

    def _read_source(self):
        """
        Read source data from GeoPackage or Shapefile using GeoPandas.
        
        Returns:
            GeoDataFrame with geometry column and attributes from the source file
        """
        input_file = os.path.join(self.path, self.file_name)
        if "gpkg" in self.source:
            return gpd.read_file(input_file, layer=self.layer, engine="pyogrio")
        return gpd.read_file(input_file, engine="pyogrio")

    def _prepare_dataframe(self, gdf):
        """
        Prepare and transform raw GeoDataFrame for database loading.
        
        Standardizes column names, enforces data types, adds UUID IDs, computes origin
        coordinates from geometry, and extracts publication metadata (version, date).
        
        Args:
            gdf: Raw GeoDataFrame from source
            
        Returns:
            Transformed GeoDataFrame with proper schema and ordered columns
        """
        gdf = gdf.rename(
            columns={
                "ex_name": "example_name",
                "ex_class": "example_class",
                "sheet_code": "sheet_code",
                "sheet_name": "sheet_name",
                "edition": "edition",
                "t50_fid": "t50_fid",
            }
        )

        if "FID" in gdf.columns:
            gdf = gdf.drop(columns=["FID"])

        string_columns = ["example_name", "example_class", "sheet_code", "sheet_name", "edition"]
        int64_columns = ["t50_fid"]

        for col in string_columns:
            if col in gdf.columns:
                gdf[col] = gdf[col].astype("string")

        for col in int64_columns:
            if col in gdf.columns:
                gdf[col] = pd.to_numeric(gdf[col], errors="coerce").astype("Int64")

        gdf.insert(0, "id", [uuid.uuid4() for _ in range(len(gdf))])
        gdf["type"] = self.table_name

        origins = gdf.geometry.apply(self.top_left_from_geometry)
        origins_df = origins.apply(pd.Series)
        gdf["origin_x"] = pd.to_numeric(origins_df[0], errors="coerce").round(0).astype("Int32")
        gdf["origin_y"] = pd.to_numeric(origins_df[1], errors="coerce").round(0).astype("Int32")

        if "example_point_id" not in gdf.columns:
            gdf["example_point_id"] = pd.NA

        if "edition" in gdf.columns:
            edition_series = gdf["edition"].astype("string")
            gdf["published_version"] = edition_series.str.extract(
                r"Edition\s+([0-9]+(?:\.[0-9]+)?)",
                expand=False,
            )
            published_year = pd.to_numeric(
                edition_series.str.extract(r"Published\s+([0-9]{4})", expand=False),
                errors="coerce",
            ).astype("Int64")
            published_at = pd.to_datetime(published_year.astype("string") + "-01-01", errors="coerce")
            gdf["published_at"] = published_at.dt.date
        else:
            gdf["published_version"] = pd.NA
            gdf["published_at"] = pd.NA

        gdf["updated_at"] = gdf["published_at"]

        ordered_columns = [col for col in self.dtype_mapping.keys() if col in gdf.columns]
        geometry_column = gdf.geometry.name
        if geometry_column in gdf.columns:
            ordered_columns.append(geometry_column)
        return gdf[ordered_columns]

    def _update_example_point_ids(self, conn):
        """
        Populate example_point_id by joining trig_point and geographic_name tables.
        
        Executes two UPDATE statements:
        1. Join trig_point table by code for 'trig_pnt' examples
        2. Join geographic_name table by name for 'geographic_name' examples
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.{self.table_name} ms
                SET example_point_id = tp.{self.topo_id_name}
                FROM {self.release_schema}.trig_point tp
                WHERE tp.code = ms.example_name
                  AND ms.example_class = 'trig_pnt';
                """
            )
        )

        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.{self.table_name} ms
                SET example_point_id = gn.{self.topo_id_name}
                FROM {self.release_schema}.geographic_name gn
                WHERE gn.name = ms.example_name
                  AND ms.example_class = 'geographic_name';
                """
            )
        )

    def _create_example_point_ids_lookup(self, conn):
        """
        Create lookup table combining trig_point and geographic_name with spatial coordinates.
        
        Generates lookups.example_point_ids table with geometry transformed to EPSG:2193 (NZTM),
        enabling proximity-based matching for ambiguous place names. Combines both trig_point
        and geographic_name sources via UNION.
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                DROP TABLE IF EXISTS lookups.example_point_ids;
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE lookups.example_point_ids AS
                SELECT
                  tp.{self.topo_id_name} AS id,
                  ST_Transform(tp.geometry, 2193) AS geometry,
                  ms.example_name,
                  ms.example_class
                FROM {self.release_schema}.trig_point tp
                JOIN {self.carto_schema}.{self.table_name} ms
                  ON tp.code = ms.example_name
                  AND ms.example_class = 'trig_pnt'
                UNION ALL
                SELECT
                  gn.{self.topo_id_name} AS id,
                  ST_Transform(gn.geometry, 2193) AS geometry,
                  ms.example_name,
                  ms.example_class
                FROM {self.release_schema}.geographic_name gn
                JOIN {self.carto_schema}.{self.table_name} ms
                  ON gn.name = ms.example_name
                  AND ms.example_class = 'geographic_name';
                """
            )
        )

    def _reformat_carto_text_table(self, conn):
        """
        Rebuild carto_text table to position example_point_id as the second column after id.
        
        Uses PL/pgSQL dynamic SQL to create a temporary table with desired column ordering,
        then replaces the original table. This ensures consistent schema regardless of
        when the column was added during the pipeline.
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                DO $$
                DECLARE
                    schema_name TEXT := '{self.carto_schema}';
                    base_table TEXT := 'nztopo50_carto_text';
                    temp_table TEXT := 'nztopo50_carto_text_tmp_reorder';
                    other_cols TEXT;
                    has_example_point_id BOOLEAN;
                    select_list TEXT;
                BEGIN
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = schema_name
                          AND table_name = base_table
                          AND column_name = 'example_point_id'
                    ) INTO has_example_point_id;

                    SELECT string_agg(format('ct.%I', c.column_name), ', ' ORDER BY c.ordinal_position)
                    INTO other_cols
                    FROM information_schema.columns c
                    WHERE c.table_schema = schema_name
                      AND c.table_name = base_table
                      AND c.column_name NOT IN ('id', 'example_point_id');

                    select_list :=
                        CASE
                            WHEN has_example_point_id THEN 'ct.id, ct.example_point_id'
                            ELSE 'ct.id, NULL::uuid AS example_point_id'
                        END;

                    IF other_cols IS NOT NULL THEN
                        select_list := select_list || ', ' || other_cols;
                    END IF;

                    EXECUTE format('DROP TABLE IF EXISTS %I.%I', schema_name, temp_table);

                    EXECUTE format(
                        'CREATE TABLE %I.%I AS SELECT %s FROM %I.%I ct',
                        schema_name,
                        temp_table,
                        select_list,
                        schema_name,
                        base_table
                    );

                    EXECUTE format('DROP TABLE %I.%I', schema_name, base_table);
                    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', schema_name, temp_table, base_table);
                    EXECUTE format('ALTER TABLE %I.%I ADD PRIMARY KEY (id)', schema_name, base_table);
                END $$;
                """
            )
        )
    def _add_example_point_id_field(self, conn):
        """
        Add example_point_id UUID column to carto_text table if it doesn't exist.
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                ALTER TABLE {self.carto_schema}.nztopo50_carto_text
                ADD COLUMN IF NOT EXISTS example_point_id UUID;
                """
            )
        )


    def _update_carto_text_via_name(self, conn):
        """
        Update carto_text example_point_id for unambiguous name matches.
        
        Only updates records where a place name in carto_text matches exactly one
        example_id in the lookup table (name appears only once geographically).
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.nztopo50_carto_text ct
                SET example_point_id = ei.id
                FROM lookups.example_point_ids ei
                WHERE ct.full_text = ei.example_name
                  AND ei.id IN (
                    SELECT id
                    FROM (
                        SELECT ei2.id, count(*) AS count
                        FROM {self.carto_schema}.nztopo50_carto_text ct2
                        JOIN lookups.example_point_ids ei2
                          ON ct2.full_text = ei2.example_name
                        GROUP BY ei2.id
                    ) counts
                    WHERE count = 1
                  );
                """
            )
        )
        
    def _update_carto_text_via_geom(self, conn):
        """
        Update carto_text example_point_id for ambiguous names using spatial proximity.
        
        For place names that map to multiple geographic points, selects the closest
        example_id within 200m distance using PostGIS ST_DWithin and ST_Distance.
        Uses DISTINCT ON to ensure one match per carto_text record.
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                WITH ambiguous_points AS (
                    SELECT ei.id, ei.geometry, ei.example_name, ei.example_class
                    FROM lookups.example_point_ids ei
                    WHERE ei.id IN (
                        SELECT ei2.id
                        FROM {self.carto_schema}.nztopo50_carto_text ct
                        JOIN lookups.example_point_ids ei2
                            ON ct.full_text = ei2.example_name
                        GROUP BY ei2.id
                        HAVING COUNT(*) > 1
                    )
                ),
                closest_matches AS (
                    SELECT DISTINCT ON (ct.id)
                        ct.id            AS carto_text_id,
                        ap.id            AS example_id
                    FROM {self.carto_schema}.nztopo50_carto_text ct
                    JOIN ambiguous_points ap
                        ON ct.full_text = ap.example_name
                       AND ST_DWithin(ct.geometry, ap.geometry, 200)
                    ORDER BY ct.id, ST_Distance(ct.geometry, ap.geometry)
                )
                UPDATE {self.carto_schema}.nztopo50_carto_text ct
                SET example_point_id = cm.example_id
                FROM closest_matches cm
                WHERE ct.id = cm.carto_text_id;
                """
            )
        )
        
    def _data_fixes(self, conn):
        """
        Apply data quality fixes to example_name field.
        
        Executes three UPDATE statements:
        1. Replace 'Mt ' prefix with 'Mount ' for unmatched examples (Mt Albert → Mount Albert)
        2. Remap code names (A0TR→A0U2, AP8Y→A4UX)
        3. Fix macron/diacritical marks (Putata→Pūtata, Pohoi→Pōhoi, etc.)
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.{self.table_name} ms
                SET example_name = regexp_replace(example_name, '^Mt\\s+', 'Mount ')
                WHERE example_point_id IS NULL
                  AND example_name ~ '^Mt\\s+';
                """
            )
        )
        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.{self.table_name} ms
                SET example_name = CASE
                    WHEN example_name = 'A0TR' THEN 'A0U2'
                    WHEN example_name = 'AP8Y' THEN 'A4UX'
                    ELSE example_name
                END
                WHERE example_name IN ('A0TR', 'AP8Y');
                """
            )
        )
        conn.execute(
            text(
                f"""
                UPDATE {self.carto_schema}.{self.table_name} ms
                SET example_name = CASE
                    WHEN example_name = 'Putata' THEN 'Pūtata'
                    WHEN example_name = 'Pohoi' THEN 'Pōhoi'
                    WHEN example_name = 'Rahuimokairoa' THEN 'Rāhuimōkairoa'
                    ELSE example_name
                END
                WHERE example_name IN ('Putata', 'Pohoi', 'Rahuimokairoa');
                """
            )
        )

    def _drop_post_update_columns(self, conn):
        """
        Drop temporary columns used during the update process.
        
        Removes example_name, example_class, edition, and revised columns
        after their values have been used for matching and updates.
        Only executes if final_drop_fields flag is True.
        
        Args:
            conn: SQLAlchemy connection object
        """
        conn.execute(
            text(
                f"""
                ALTER TABLE {self.carto_schema}.{self.table_name}
                DROP COLUMN IF EXISTS example_name,
                DROP COLUMN IF EXISTS example_class,
                DROP COLUMN IF EXISTS edition,
                DROP COLUMN IF EXISTS revised;
                """
            )
        )

    def run(self):
        """
        Execute the complete map sheet import pipeline.
        
        Orchestrates all transformation steps in sequence:
        1. Load and prepare source data
        2. Write to PostGIS
        3. Update example_point_ids from trig_point/geographic_name
        4. Apply data quality fixes
        5. Create lookup table for carto_text matching
        6. Match carto_text records by name and proximity
        7. Reformat table to position example_point_id at column 2
        8. Optionally drop temporary columns
        
        All database operations are wrapped in transactions for atomicity.
        """
        gdf = self._prepare_dataframe(self._read_source())

        print(gdf.columns)
        print(f"✓ Loaded {len(gdf)} map sheet records")

        engine = create_engine(self.db_params)
        gdf.to_postgis(
            name=self.table_name,
            con=engine,
            schema=self.schema,
            if_exists="replace",
            index=False,
            dtype=self.dtype_mapping,
        )
        print("✓ Wrote map sheet data to PostGIS")

        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {self.schema}.{self.table_name} ADD PRIMARY KEY (id);
                    """
                )
            )

            conn.execute(
                text(
                    f"""
                    DELETE FROM {self.carto_schema}.{self.table_name}
                    WHERE sheet_code LIKE 'Topo%';
                    """
                )
            )
            print("✓ Added primary key and removed superfluous records")


            self._update_example_point_ids(conn)
            print("✓ Updated example_point_ids from trig_point and geographic_name")

            self._data_fixes(conn)
            print("✓ Applied data fixes (Mt->Mount, A0TR->A0U2, AP8Y->A4UX, macron names)")

            self._update_example_point_ids(conn)
            print("✓ Re-updated example_point_ids after fixes")

            conn.commit()

        with engine.begin() as conn:

            self._create_example_point_ids_lookup(conn)
            print("✓ Created lookups.example_point_ids table")

            conn.commit()

        with engine.begin() as conn: 

            self._add_example_point_id_field(conn)
            print("✓ Added example_point_id field to carto_text table")

            self._update_carto_text_via_name(conn)
            print("✓ Updated carto_text example_point_id via name match (unambiguous)")

            conn.commit()

        with engine.begin() as conn: 

            self._update_carto_text_via_geom(conn)
            print("✓ Updated carto_text example_point_id via proximity (ambiguous)")

            conn.commit()

        with engine.begin() as conn:
            
            self._reformat_carto_text_table(conn)
            print("✓ Reformatted carto_text table (example_point_id moved to column 2)")

            if self.final_drop_fields:
                self._drop_post_update_columns(conn)
                print("✓ Dropped post-update columns")

            conn.commit()

        print("✓ Data imported successfully")


if __name__ == "__main__":
    final_drop_fields = True

    # source = 'windows-shp'
    source = "windows-gpkg"
    # source = 'aws-gpkg'

    db_params = "postgresql+psycopg://postgres:landinformation@localhost:5432/topo"

    if "windows" in source:
        # path = r"C:\Data\Topo50\Topo50_carto_text_2020_09"
        path = r"C:\Data\Topo50\kart-topographic-source-data\topographic-source-data"
    else:
        path = "s3://tbc/source/lamps/linz_map_sheet"

    if source == "windows-shp":
        file_name = "linz_carto_text_2020_09.shp"
        layer = None
    else:
        file_name = "topographic-source-data.gpkg"
        layer = "linz_map_sheet"

    importer = MapSheetImporter(
        db_params=db_params,
        source=source,
        path=path,
        file_name=file_name,
        layer=layer,
        carto_schema="carto",
        release_schema="release66",
        topo_id_name="id",
        final_drop_fields=final_drop_fields,
    )
    importer.run()


