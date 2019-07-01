#!/bin/sh
echo "Start data generation"
#python3 driver/name_driver.py
python3 driver/movie_driver.py
#mv name.json json/
mv movie.json json/
echo "Data sucessfully generated.."
echo "MongoDB syntax formatting..."
mongo < javascript/db.js
echo "Sucessfully created search indexes on title..\n"

