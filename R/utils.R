open_connection <- function() {
    DBI::dbConnect(
        RPostgres::Postgres(),
        host = Sys.getenv("DB_HOST"),
        port = Sys.getenv("DB_PORT"),
        user = Sys.getenv("DB_USER"),
        password = Sys.getenv("DB_PASS"),
        dbname = Sys.getenv("DB_NAME")
    )
}

get_dim_sensores <- function(
    con = open_connection()
) {
    setDT(
        dbGetQuery(
            con,
            "
            SELECT
                id_detector,
                dsc_avenida,
                dsc_int_anterior,
                dsc_int_siguiente,
                latitud,
                longitud
            FROM public.d_sensores
            "
        )
    )
}