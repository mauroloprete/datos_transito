dim_sensores_files <- list.files(
    path = here::here(
        "raw-data"
    ),
    pattern = "ubicacion_sensores_*"
)


dim_sensores_files <- data.frame(
    nombres = dim_sensores_files,
    size_gb = sapply(
        X = here::here(
            "raw-data",
            dim_sensores_files
        ),
        FUN = function(x) {
            file.size(x) / (9.31*(10)^(-9))
        }
    )
)

dim_sensores_files$full_path <- row.names(dim_sensores_files)
row.names(dim_sensores_files) <- NULL



library(data.table)


dim_sensores_raw <- purrr::map_dfr(
    .x = dim_sensores_files$full_path,
    .f = function(path) {
        
        dt <- data.table::fread(
            file = path
        )

        cols <- c(
            "dsc_avenida",
            "dsc_int_anterior",
            "dsc_int_siguiente",
            "latitud",
            "longitud"
        )

        print(path)

        dim <- dt[
            ,
            id_detector := .GRP,
            by = cols
        ][
            ,
            c(names(dt))[!names(dt) %in% c("id_detector", cols)] := NULL
        ][
            ,
            .SD[1],
            by = id_detector
        ]

        return(
            data.table(dim)
        )
    }
)

dim_sensores <- dim_sensores_raw[
    ,
    id_detector := .GRP,
    by = c(
        "dsc_avenida",
        "dsc_int_anterior",
        "dsc_int_siguiente",
        "latitud",
        "longitud"
    )
][
    ,
    .SD[1],
    by = id_detector
]


con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("DB_HOST"),
    port = Sys.getenv("DB_PORT"),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASS"),
    dbname = Sys.getenv("DB_NAME")
)


DBI::dbAppendTable(
    con,
    name = DBI::Id(
        schema = "public",
        table = "d_sensores"
    ),
    dim_sensores
)

