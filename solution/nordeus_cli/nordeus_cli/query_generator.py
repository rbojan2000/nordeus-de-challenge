class QueryGenerator:
    @classmethod
    def get_query(cls, level: str, date: str, user_id: str) -> str:
        if level == "user-level":
            return cls.generate_user_level_stats_query(date=date, user_id=user_id)
        else:
            return cls.generate_game_level_query(date)

    @classmethod
    def generate_user_level_stats_query(cls, date: str, user_id: str) -> str:
        query = ""
        if date:
            query = f"""
                WITH game_stats AS (
                    WITH home_stats AS (
                        SELECT
                            home_user_id AS user_id,
                            SUM(match_duration) AS time_spent_in_game_home,
                            SUM(home_user_points) AS total_points_won_home
                        FROM public.match_stats
                        WHERE home_user_id = '{user_id}'
                        AND DATE(start_time) = DATE('{date}')
                        GROUP BY home_user_id
                    ),
                    away_stats AS (
                        SELECT
                            away_user_id AS user_id,
                            SUM(match_duration) AS time_spent_in_game_away,
                            SUM(away_user_points) AS total_points_won_away
                        FROM public.match_stats
                        WHERE away_user_id = '{user_id}'
                        AND DATE(start_time) = DATE('{date}')
                        GROUP BY away_user_id
                    )
                    SELECT
                        COALESCE(h.user_id, a.user_id) AS user_id,
                        COALESCE(h.time_spent_in_game_home, 0) + COALESCE(a.time_spent_in_game_away, 0) AS time_spent_in_game,
                        COALESCE(h.total_points_won_home, 0) AS total_points_won_home,
                        COALESCE(a.total_points_won_away, 0) AS total_points_won_away
                    FROM home_stats h
                    FULL OUTER JOIN away_stats a
                    ON h.user_id = a.user_id
                ),
                session_stats AS (
                    WITH user_logins AS (
                        SELECT
                            user_id,
                            MAX(session_timestamp) AS last_login
                        FROM user_session_stats
                        WHERE is_new_session = true
                        AND user_id = '{user_id}'
                        AND DATE(session_timestamp) <= DATE('{date}')
                        GROUP BY user_id
                    ),
                    sessions_count AS (
                        SELECT
                            user_id,
                            COUNT(*) AS session_count
                        FROM user_session_stats
                        WHERE DATE(session_timestamp) = DATE('{date}')
                        AND user_id = '{user_id}'
                        AND is_new_session = true
                        GROUP BY user_id
                    ),
                    session_duration as (
                        SELECT
                                user_id,
                                COUNT(*) * 60 AS total_seconds
                        FROM user_session_stats
                        WHERE is_new_session = False
                        AND user_id = '{user_id}'
                        AND DATE(session_timestamp) = DATE('{date}')
                        group by(user_id)
                    ),
                    registration_info AS (
                        SELECT
                            user_id,
                            registration_local_datetime,
                            country
                        FROM user_session_stats
                        WHERE user_id = '{user_id}'
                        LIMIT 1
                    )
                    SELECT
                        u.user_id,
                        DATE('{date}') - DATE(u.last_login) AS days_since_last_login,
                        COALESCE(d.session_count, 0) AS number_of_sessions,
                        COALESCE(sd.total_seconds, 0) AS total_session_duration,
                        r.registration_local_datetime,
                        r.country
                    FROM user_logins u
                    LEFT JOIN sessions_count d ON u.user_id = d.user_id
                    LEFT JOIN session_duration sd ON u.user_id = sd.user_id
                    LEFT JOIN registration_info r on u.user_id = r.user_id
                )
                SELECT
                    s.user_id,
                    s.country,
                    s.registration_local_datetime,
                    s.days_since_last_login,
                    s.number_of_sessions,
                    COALESCE(g.time_spent_in_game, 0) AS time_spent_in_game,
                    COALESCE(g.total_points_won_home, 0) AS total_points_won_home,
                    COALESCE(g.total_points_won_away, 0) AS total_points_won_away,
                    CASE
                        WHEN COALESCE(s.total_session_duration, 0) > 0 THEN
                            (COALESCE(CAST(g.time_spent_in_game AS FLOAT), 0) / CAST(s.total_session_duration AS FLOAT)) * 100
                        ELSE 0
                    END AS match_time_as_percentage_of_total_game_time
                FROM game_stats g
                FULL JOIN session_stats s ON g.user_id = s.user_id;
            """
        else:
            query = f"""
                WITH game_stats AS (
                    WITH home_stats AS (
                        SELECT
                            home_user_id AS user_id,
                            SUM(match_duration) AS time_spent_in_game_home,
                            SUM(home_user_points) AS total_points_won_home
                        FROM public.match_stats
                        WHERE home_user_id = '{user_id}'
                        GROUP BY home_user_id
                    ),
                    away_stats AS (
                        SELECT
                            away_user_id AS user_id,
                            SUM(match_duration) AS time_spent_in_game_away,
                            SUM(away_user_points) AS total_points_won_away
                        FROM public.match_stats
                        WHERE away_user_id = '{user_id}'
                        GROUP BY away_user_id
                    )
                    SELECT
                        COALESCE(h.user_id, a.user_id) AS user_id,
                        COALESCE(h.time_spent_in_game_home, 0) + COALESCE(a.time_spent_in_game_away, 0) AS time_spent_in_game,
                        COALESCE(h.total_points_won_home, 0) AS total_points_won_home,
                        COALESCE(a.total_points_won_away, 0) AS total_points_won_away
                    FROM home_stats h
                    FULL OUTER JOIN away_stats a
                    ON h.user_id = a.user_id
                ),
                session_stats AS (
                    WITH user_logins AS (
                        SELECT
                            user_id,
                            MAX(session_timestamp) AS last_login
                        FROM user_session_stats
                        WHERE is_new_session = true
                        AND user_id = '{user_id}'
                        GROUP BY user_id
                    ),
                    sessions_count AS (
                        SELECT
                            user_id,
                            COUNT(*) AS session_count
                        FROM user_session_stats
                        where user_id = '{user_id}'
                        AND is_new_session = true
                        GROUP BY user_id
                    ),
                    session_duration as (
                        SELECT
                            user_id,
                            COUNT(*) * 60 AS total_seconds
                        FROM user_session_stats
                        WHERE is_new_session = False
                        AND user_id = '{user_id}'
                        group by(user_id)
                    ),
                    registration_info AS (
                        SELECT
                            user_id,
                            registration_local_datetime,
                            country
                        FROM user_session_stats
                        WHERE user_id = '{user_id}'
                        LIMIT 1
                    )
                    SELECT
                        u.user_id,
                        DATE(NOW()) - DATE(u.last_login) AS days_since_last_login,
                        COALESCE(d.session_count, 0) AS number_of_sessions,
                        COALESCE(sd.total_seconds, 0) AS total_session_duration,
                        r.registration_local_datetime,
                        r.country
                    FROM user_logins u
                    LEFT JOIN sessions_count d ON u.user_id = d.user_id
	                LEFT JOIN session_duration sd ON u.user_id = sd.user_id
                    LEFT JOIN registration_info r on u.user_id = r.user_id
                )
                SELECT
                    s.user_id,
                    s.country,
                    s.registration_local_datetime,
                    s.days_since_last_login,
                    s.number_of_sessions,
                    COALESCE(g.time_spent_in_game, 0) AS time_spent_in_game,
                    COALESCE(g.total_points_won_home, 0) AS total_points_won_home,
                    COALESCE(g.total_points_won_away, 0) AS total_points_won_away,
                    CASE
                        WHEN COALESCE(s.total_session_duration, 0) > 0 THEN
                            (COALESCE(CAST(g.time_spent_in_game AS FLOAT), 0) / CAST(s.total_session_duration AS FLOAT)) * 100
                        ELSE 0
                    END AS match_time_as_percentage_of_total_game_time
                FROM game_stats g
                FULL JOIN session_stats s ON g.user_id = s.user_id;
            """

        return query

    @classmethod
    def generate_game_level_query(cls, date: str) -> str:
        if date:
            query = f"""
                WITH session_stats AS (
                    WITH active_users AS (
                        SELECT
                            COUNT(DISTINCT user_id) AS active_users
                        FROM user_session_stats
                        WHERE DATE(session_timestamp) = DATE('{date}')
                        AND is_new_session = true
                    ),
                    number_of_sessions AS (
                        SELECT
                            COUNT(*) AS number_of_sessions
                        FROM user_session_stats
                        WHERE DATE(session_timestamp) = DATE('{date}')
                        AND is_new_session = true
                    ),
                    user_sessions_on_date AS (
                        SELECT
                            user_id,
                            COUNT(*) AS session_count
                        FROM user_session_stats
                        WHERE DATE(session_timestamp) = DATE('{date}')
                        AND is_new_session = true
                        GROUP BY user_id
                    ),
                    average_number_of_sessions AS (
                        SELECT
                            COALESCE(AVG(session_count), 0) AS average_number_of_sessions
                        FROM user_sessions_on_date
                    )
                    SELECT
                        (SELECT active_users FROM active_users) AS active_users,
                        (SELECT number_of_sessions FROM number_of_sessions) AS number_of_sessions,
                        (SELECT average_number_of_sessions FROM average_number_of_sessions) AS average_number_of_sessions
                ),
                game_stats AS (
                    WITH user_total_points AS (
                        WITH home_stats AS (
                            SELECT
                                home_user_id AS user_id,
                                SUM(home_user_points) AS total_points_won_home
                            FROM public.match_stats
                            WHERE DATE(start_time) = DATE('{date}')
                            GROUP BY home_user_id
                        ),
                        away_stats AS (
                            SELECT
                                away_user_id AS user_id,
                                SUM(away_user_points) AS total_points_won_away
                            FROM public.match_stats
                            WHERE DATE(start_time) = DATE('{date}')
                            GROUP BY away_user_id
                        )
                        SELECT
                            COALESCE(h.user_id, a.user_id) AS user_id,
                            COALESCE(h.total_points_won_home, 0) + COALESCE(a.total_points_won_away, 0) AS total_points_won
                        FROM home_stats h
                        FULL OUTER JOIN away_stats a
                        ON h.user_id = a.user_id
                    ),
                    max_points AS (
                        SELECT
                            MAX(total_points_won) AS max_points_won
                        FROM user_total_points
                    )
                    SELECT
                        user_id
                    FROM user_total_points
                    WHERE total_points_won = (SELECT max_points_won FROM max_points)
                )
                SELECT
				    COALESCE(ss.active_users, 0) AS active_users,
				    COALESCE(ss.number_of_sessions, 0) AS number_of_sessions,
				    COALESCE(ss.average_number_of_sessions, 0) AS average_number_of_sessions,
				    ARRAY_AGG(CAST (gs.user_id AS TEXT)) AS users_with_max_points
                FROM
                    session_stats ss
                CROSS JOIN
                    game_stats gs
                group by ss.active_users, ss.number_of_sessions, ss.average_number_of_sessions
            """
        else:
            query = f"""
                WITH session_stats AS (
                    WITH active_users AS (
                        SELECT
                            COUNT(DISTINCT user_id) AS active_users
                        FROM user_session_stats
                        WHERE is_new_session = true
                    ),
                    number_of_sessions AS (
                        SELECT
                            COUNT(*) AS number_of_sessions
                        FROM user_session_stats
                        where is_new_session = true
                    ),
                    user_sessions_on_date AS (
                        SELECT
                            user_id,
                            COUNT(*) AS session_count
                        FROM user_session_stats
                        where is_new_session = true
                        GROUP BY user_id
                    ),
                    average_number_of_sessions AS (
                        SELECT
                            COALESCE(AVG(session_count), 0) AS average_number_of_sessions
                        FROM user_sessions_on_date
                    )
                    SELECT
                        (SELECT active_users FROM active_users) AS active_users,
                        (SELECT number_of_sessions FROM number_of_sessions) AS number_of_sessions,
                        (SELECT average_number_of_sessions FROM average_number_of_sessions) AS average_number_of_sessions
                ),
                game_stats AS (
                    WITH user_total_points AS (
                        WITH home_stats AS (
                            SELECT
                                home_user_id AS user_id,
                                SUM(home_user_points) AS total_points_won_home
                            FROM public.match_stats
                            GROUP BY home_user_id
                        ),
                        away_stats AS (
                            SELECT
                                away_user_id AS user_id,
                                SUM(away_user_points) AS total_points_won_away
                            FROM public.match_stats
                            GROUP BY away_user_id
                        )
                        SELECT
                            COALESCE(h.user_id, a.user_id) AS user_id,
                            COALESCE(h.total_points_won_home, 0) + COALESCE(a.total_points_won_away, 0) AS total_points_won
                        FROM home_stats h
                        FULL OUTER JOIN away_stats a
                        ON h.user_id = a.user_id
                    ),
                    max_points AS (
                        SELECT
                            MAX(total_points_won) AS max_points_won
                        FROM user_total_points
                    )
                    SELECT
                        user_id
                    FROM user_total_points
                    WHERE total_points_won = (SELECT max_points_won FROM max_points)
                )
                SELECT
                    ss.active_users,
                    ss.number_of_sessions,
                    ss.average_number_of_sessions,
                    ARRAY_AGG(CAST (gs.user_id AS TEXT)) AS users_with_max_points
                FROM
                    session_stats ss,
                    game_stats gs
                group by ss.active_users, ss.number_of_sessions, ss.average_number_of_sessions

            """
        return query
