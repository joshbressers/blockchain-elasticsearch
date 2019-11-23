#!/bin/bash

while true;
do
echo "Updating"
date
./add-price.py
echo "Done"
sleep 86400
done
