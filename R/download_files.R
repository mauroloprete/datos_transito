download.file(
    url = "https://docs.google.com/spreadsheets/d/1bGnAO7T4AC1MorSyI1N41vLx2zUn30fahdGtpYFNoy8/export?format=csv&id=1bGnAO7T4AC1MorSyI1N41vLx2zUn30fahdGtpYFNoy8&gid=0",
    destfile = here::here("links_archivos.csv")
)

tbl <- vroom::vroom(
    here::here("links_archivos.csv"),
    delim = ","
)

mapply(
    url = tbl$url_csv,
    destfile = here::here(
        "raw-data",
        paste0(
            tbl$nombre_tabla,
            "_",
            tbl$mes,
            tbl$anio,
            ".",
            tbl$ext
        )
    ),
    FUN = download.file
)