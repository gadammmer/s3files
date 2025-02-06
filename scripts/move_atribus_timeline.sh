
#!/bin/bash
source /opt/GEAOSP_PY_POC_TURISMO/venv_invattur/bin/activate

platforms=("twitter")
platforms+=("youtube" "news" "facebook" "blog" "forum" "web"  "instagram" "reddit" "linkedin" "tiktok" "tvradio")
#platforms+=("tumblr" "mybusiness" "vimeo" "tripadvisor")

start_date=$(date -d "yesterday" '+%Y-%m-%d 00:00:00')
end_date=$(date -d "today" '+%Y-%m-%d 00:00:00')


for platform in "${platforms[@]}"; do
    python /opt/GEAOSP_PY_POC_TURISMO/scripts/move_atribus_timeline.py "$start_date" "$end_date" "$platform"
done


