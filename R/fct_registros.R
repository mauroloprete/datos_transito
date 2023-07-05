library(magrittr)
library(data.table)
library(DBI)

vel_media_to_fact <- function(year_month,dim_sensores) {

    vel_media <- fread(
        here::here(
            "raw-data",
            paste0(
                "velocidad_media_vehicular_",
                year_month,
                ".csv"
            )
        )
    )

    if ("velocidad_promedio" %in% names(vel_media)) {
        setnames(vel_media,"velocidad_promedio","velocidad",skip_absent = TRUE)
    }

    cols_vel_media = c(
        "cod_detector",
        "id_carril",
        "fecha",
        "hora",
        "dsc_avenida",
        "dsc_int_anterior",
        "dsc_int_siguiente",
        "latitud",
        "longitud",
        "velocidad"
    )


    validacion = isTRUE(
        all.equal(
            sort(names(vel_media)),
            sort(cols_vel_media)
        )
    )

    if (!validacion) {
        stop(message("Columnas no validas velociad media"))
    } else {

        return(raw_to_fact(vel_media,dim_sensores = dim_sensores))
    }
}


conteo_vehicular_to_fact <- function(year_month,dim_sensores) {

    conteo_vehicular <- fread(
        here::here(
            "raw-data",
            paste0(
                "conteo_vehicular_",
                year_month,
                ".csv"
            )
        )
    )

    cols_conteo_vehicular <- c(
        "cod_detector",
        "id_carril",
        "fecha",
        "hora",
        "dsc_avenida",
        "dsc_int_anterior",
        "dsc_int_siguiente",
        "latitud",
        "longitud",
        "volume",
        "volumen_hora"
    )


    validacion = isTRUE(
        all.equal(
            sort(cols_conteo_vehicular),
            sort(names(conteo_vehicular))
        )
    )


    if (!validacion) {
        message("Columnas no validas conteo vehicular")
    } else {
        return(raw_to_fact(conteo_vehicular,dim_sensores = dim_sensores))
    }

}


raw_to_fact <- function(dt,dim_sensores) {
        dt[
        ,
        `:=` (
            id_fecha = (
                as.numeric(format(as.Date(fecha),"%Y")) * 10000 + 
                    as.numeric(format(as.Date(fecha),"%m")) * 100 + 
                    as.numeric(format(as.Date(fecha), "%d"))
            ),
            id_hora = (
                gsub(
                    ":",
                    "",
                    substr(
                        hora,
                        start = 1,
                        stop = 5
                    )
                )
            ),
            latitud = latitud,
            longitud = longitud
        )
    ][
        ,
        c(
            "cod_detector",
            "fecha",
            "hora"
        ) := NULL
    ]

    keys = c(
        "dsc_avenida",
        "dsc_int_anterior",
        "dsc_int_siguiente",
        "latitud",
        "longitud"
    )

    setkeyv(
        dt,
        keys   
    )

    setkeyv(
        dim_sensores,
        keys
    )

    tmp = merge(
        x = dt,
        y = dim_sensores,
        all.x = TRUE
    )

    tmp[
        ,
        c(names(dt))[names(dt) %in% keys] := NULL
    ]

    return(data.table(tmp))
}

load_fact_table <- function(
    con,
    year_month
) {

    dim_sensores = get_dim_sensores(
        con = con
    )

    vel_media = vel_media_to_fact(
        year_month,
        dim_sensores
    )


    conteo_vehicular = conteo_vehicular_to_fact(
        year_month,
        dim_sensores
    )

    nrow_validate = (nrow(vel_media) == nrow(conteo_vehicular))

    if (nrow_validate) {

        keys = c(
            "id_carril",
            "id_fecha",
            "id_hora",
            "id_detector"
        )

        fct = merge(
            x = conteo_vehicular,
            y = vel_media
        )

        DBI::dbAppendTable(
            con,
            name = DBI::Id(
                schema = "public",
                table = "fct_registros"
            ),
            fct
        )
    } else {
        message("No hay la misma cantidad de filas!")
    }

}

source(
    here::here("R","utils.R")
)

con = open_connection()

year_month <- na.omit(
    gsub(
        ".csv",
        "",
        unique(
            sapply(
                X = list.files(
                    here::here(
                        "raw-data"
                    )
                ),
                FUN = function(x) {
                    {
                        strsplit(
                            x,
                            "_"
                        )
                    }[[1]][4]
                }
            )
        )
    )
)



library(furrr)

plan(multisession, workers = 4)

future_walk(
    .x = year_month,
    .f = function(x) {
        load_fact_table(
            con = open_connection(),
            year_month = x
        )
    }
)
