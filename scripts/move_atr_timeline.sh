#!/bin/bash
survey=$1

# Define la ruta del bucket S3 y la ruta de destino local
s3_path="s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/"
local_path="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_timeline/${survey}"
csv_output_path="/opt/GEAOSP_PY_POC_TURISMO/scripts/files/atribus_timeline/${survey}/csv_output"

# Crea el directorio si no existe
mkdir -p "$local_path"
mkdir -p "$csv_output_path"

# Descarga los archivos .json que contengan el valor de $survey
aws s3 ls "$s3_path" --recursive | grep "$survey" | grep ".json" | awk '{print $4}' | while read file; do
    # Descarga el archivo .json en la ruta local
    aws s3 cp "s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/$file" "$local_path/"
    
    # Obtener el nombre base del archivo sin extensión
    base_name=$(basename "$file" .json)

    # Comprobar si el archivo es de YouTube (si el nombre del archivo contiene "youtube")
    if [[ "$base_name" == *youtube* ]]; then
        # Convertir el JSON a CSV solo para archivos YouTube
        jq -r '.items[] | [
            .sentiment,
            (.comments | tojson),
            .dislikeCount,
            .idSearch,
            .language,
            .likeCount,
            "\"\(.title)\"",
            .idCategory,
            .commentCount,
            (.tags | tojson),
            .duration,
            .displayUrl,
            .license,
            .dateCreated,
            (.stats | tojson),
            (.entities | tojson),
            .id,
            "\"\(.text)\"",
            .viewCount,
            .category,
            (.user | tojson),
            .favoriteCount
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo YouTube convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/
    elif [[ "$base_name" == *facebook* ]]; then
        jq -c '.items | .[]' "$local_path/$base_name.json" | split -l 100 - "$csv_output_path/part_"

        for file in "$csv_output_path/part_"*; do
            jq -r '
            . | [
                .sentiment,
                (if (.comments | length) > 0 then (.comments | tojson) else "[]" end),
                (if (.ocrs | length) > 0 then (.ocrs | tojson) else "[]" end),
                .sourceTitle,
                .idSearch,
                .language,
                .likeCount,
                .media,
                .idCategory,
                .reactionCount,
                .commentCount,
                (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),
                .sourceUrl,
                .displayUrl,
                .shareCount,
                .dateCreated,
                (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),
                (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),
                .id,
                .text,
                .sourceDescription,
                .category,
                (if (.user | length) > 0 then (.user | tojson) else "{}" end)
            ] | @csv' "$file" >> "$csv_output_path/$base_name.csv"
        done


        echo "Archivo Facebook convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    elif [[ "$base_name" == *instagram* ]]; then
    
        jq -r '
        .items[] | [
            .country,  # COUNTRY
            .sentiment,  # SENTIMENT
            (if (.comments | length) > 0 then (.comments | tojson) else "[]" end),  # COMMENTS
            (if (.ocrs | length) > 0 then (.ocrs | tojson) else "[]" end),  # OCRS
            .latitude,  # LATITUDE
            .county,  # COUNTY
            .idSearch,  # IDSEARCH
            .language,  # LANGUAGE
            .likeCount,  # LIKECOUNT
            .media,  # MEDIA
            .idCategory,  # IDCATEGORY
            .commentCount,  # COMMENTCOUNT
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            (if .isStory then true else false end),  # ISSTORY
            .id,  # ID
            .text,  # TEXT
            .viewCount,  # VIEWCOUNT
            .category,  # CATEGORY
            (if (.user | length) > 0 then (.user | tojson) else "{}" end),  # USUARIO
            .longitude  # LONGITUDE
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo Instagram convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    elif [[ "$base_name" == *twitter* ]]; then
        echo "Hay que hacer la estructura del json de Twiiter"    

    elif [[ "$base_name" == *tiktok* ]]; then
        jq -r '
        .items[] | [
            .sentiment,  # SENTIMENT
            .idSearch,  # IDSEARCH
            .language,  # LANGUAGE
            .likeCount,  # LIKECOUNT
            .media,  # MEDIA
            .idCategory,  # IDCATEGORY
            .commentCount,  # COMMENTCOUNT
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .playCount,  # PLAYCOUNT
            .shareCount,  # SHARECOUNT
            .dateCreated,  # DATECREATED
            (if (.music | length) > 0 then (.music | tojson) else "{}" end),  # MUSIC
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .id,  # ID
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end)  # USUARIO
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"
    
        echo "Archivo TikTok convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/
    elif [[ "$base_name" == *reddit* ]]; then
        jq -r '
        .items[] | [
            .gildedCount,  # GLIDEDCOUNT
            .sentiment,  # SENTIMENT
            (if (.comments | length) > 0 then (.comments | tojson) else "[]" end),  # COMMENTS
            (if (.ocrs | length) > 0 then (.ocrs | tojson) else "[]" end),  # OCR
            .upCount,  # UPCOUNT
            .idSearch,  # IDSEARCH
            .language,  # LANGUAGE
            .media,  # MEDIA
            .title,  # TITLE
            .idCategory,  # IDCATEGORY
            .subreddit,  # SUBREDDIT
            .commentCount,  # COMMENTCOUNT
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .sourceUrl,  # SOURCEURL
            .subredditType,  # SUBREDDITTYPE
            .downCount,  # DOWNCOUNT
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .domain,  # DOMAIN
            .id,  # ID
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end)  # USUARIO
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo Reddit convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/
    elif [[ "$base_name" == *news* ]]; then
        
        jq -r '
        .items[] | [
            .country,  # COUNTRY
            .sentiment,  # SENTIMENT
            .latitude,  # LATITUDE
            .county,  # COUNTY
            .sourceTitle,  # SOURCETITLE
            .idSearch,  # IDSEARCH
            .source,  # SOURCE
            .media,  # MEDIA
            .title,  # TITLE
            .idCategory,  # IDCATEGORY
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .shareCount,  # SHARECOUNT
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .street,  # STREET
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end),  # USUARIO
            .longitude,  # LONGITUD
            (if (.ocr | length) > 0 then (.ocr | tojson) else "[]" end)  # OCR
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo News convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    elif [[ "$base_name" == *forum* ]]; then

        jq -r '
        .items[] | [
            .country,  # COUNTRY
            .sentiment,  # SENTIMENT
            .latitude,  # LATITUDE
            .county,  # COUNTY
            .sourceTitle,  # SOURCETITLE
            .idSearch,  # IDSEARCH
            .source,  # SOURCE
            .media,  # MEDIA
            .title,  # TITLE
            .idCategory,  # IDCATEGORY
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .shareCount,  # SHARECOUNT
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .street,  # STREET
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end),  # USUARIO
            .longitude,  # LONGITUD
            (if (.ocr | length) > 0 then (.ocr | tojson) else "[]" end)  # OCR
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo Forum convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    elif [[ "$base_name" == *web* ]]; then

        jq -r '
        .items[] | [
            .country,  # COUNTRY
            .sentiment,  # SENTIMENT
            .latitude,  # LATITUDE
            .county,  # COUNTY
            .sourceTitle,  # SOURCETITLE
            .idSearch,  # IDSEARCH
            .source,  # SOURCEURL
            .media,  # MEDIA
            .title,  # TITLE
            .idCategory,  # IDCATEGORY
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .shareCount,  # SHARECOUNT
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .street,  # STREET
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end),  # USUARIO
            .longitude,  # LONGITUD
            (if (.ocr | length) > 0 then (.ocr | tojson) else "[]" end)  # OCR
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo Web convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    elif [[ "$base_name" == *blog* ]]; then
        jq -r '
        .items[] | [
            .country,  # COUNTRY
            .sentiment,  # SENTIMENT
            .latitude,  # LATITUDE
            .county,  # COUNTY
            .sourceTitle,  # SOURCETITLE
            .idSearch,  # IDSEARCH
            .source,  # SOURCE
            .media,  # MEDIA
            .title,  # TITLE
            .idCategory,  # IDCATEGORY
            (if (.tags | length) > 0 then (.tags | tojson) else "[]" end),  # ETIQUETAS
            .displayUrl,  # DISPLAYURL
            .shareCount,  # SHARECOUNT
            .dateCreated,  # DATECREATED
            (if (.stats | length) > 0 then (.stats | tojson) else "{}" end),  # STATS
            (if (.entities | length) > 0 then (.entities | tojson) else "[]" end),  # ENTITIES
            .street,  # STREET
            .text,  # TEXT
            (if (.user | length) > 0 then (.user | tojson) else "{}" end),  # USUARIO
            .longitude,  # LONGITUD
            (if (.ocr | length) > 0 then (.ocr | tojson) else "[]" end)  # OCR
        ] | @csv' "$local_path/$base_name.json" > "$csv_output_path/$base_name.csv"

        echo "Archivo Blog convertido: $base_name.csv"
        echo
        aws s3 cp "$csv_output_path/$base_name.csv" s3://osp-bigm-orangedataprovider-pro-s3-bucket-eu-west-1-snowflake/innvatur/atribus/timeline/csv/${survey}/

    else
        echo "Archivo no identificado, omitido: $base_name.json"
    fi
done

echo "Proceso completado."


