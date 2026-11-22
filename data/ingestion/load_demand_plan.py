from load_schedules import check_schedules, database_cursor, staged_delivery


def main():
    with database_cursor() as cursor, staged_delivery() as delivery:
        check_schedules(cursor, delivery)
        cursor.execute("""
            SELECT flow_group_id FROM public.flow_groups WHERE topology_id = %s
        """, (delivery["topology_id"],))
        missing = delivery["flow_ids"] - {row[0] for row in cursor}
        if missing:
            raise ValueError(f"Chạy load_links_flows.py trước. Thiếu flow group: {sorted(missing)}")

        cursor.execute("""
            CREATE TEMP TABLE demand_stage (
                schedule_id text, scenario_id text, seed bigint, split text, traffic_offset_s integer,
                flow_group_id text, requested_mbps double precision, pre_clip_mbps double precision
            ) ON COMMIT DROP
        """)
        with delivery["demand_file"].open(encoding="utf-8-sig", newline="") as source:
            cursor.copy_expert(
                "COPY pg_temp.demand_stage FROM STDIN WITH (FORMAT CSV, HEADER true)", source
            )

        cursor.execute("LOCK TABLE public.demand_plan IN SHARE ROW EXCLUSIVE MODE")
        cursor.execute("""
            SELECT d.schedule_id, d.flow_group_id, d.time_offset_s
            FROM public.demand_plan d
            LEFT JOIN pg_temp.demand_stage src
              ON (src.schedule_id, src.flow_group_id, src.traffic_offset_s) =
                 (d.schedule_id, d.flow_group_id, d.time_offset_s + %s)
            WHERE d.schedule_id = ANY(%s)
              AND (src.schedule_id IS NULL OR d.topology_id IS DISTINCT FROM %s
                   OR d.requested_mbps IS DISTINCT FROM src.requested_mbps)
            LIMIT 1
        """, (delivery["warmup"], list(delivery["metadata"]), delivery["topology_id"]))
        conflict = cursor.fetchone()
        if conflict:
            raise ValueError(f"Demand đã có và khác dữ liệu nguồn tại {conflict}.")

        cursor.execute("""
            INSERT INTO public.demand_plan (
                schedule_id, topology_id, flow_group_id, time_offset_s, requested_mbps
            )
            SELECT src.schedule_id, %s, src.flow_group_id, src.traffic_offset_s - %s, src.requested_mbps
            FROM pg_temp.demand_stage src
            WHERE NOT EXISTS (
                SELECT 1 FROM public.demand_plan d
                WHERE (d.schedule_id, d.flow_group_id, d.time_offset_s) =
                      (src.schedule_id, src.flow_group_id, src.traffic_offset_s - %s)
            )
        """, (delivery["topology_id"], delivery["warmup"], delivery["warmup"]))
        inserted = cursor.rowcount
    print(f"Đã nạp thêm {inserted:,} dòng vào public.demand_plan.")


if __name__ == "__main__":
    main()

