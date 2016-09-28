#!/bin/bash
#
# Copyright (C) 2016, T-System International GmbH
#
# Author: Volker Zahn <volker.zahn@t-systemns.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
# writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
#

if [ "$#" -eq "3" ])
then
	database=$2
	user=$3
	password=$4
elif [ "$#" -eq "0" ]
then
	database=bonn
	user=osmuser
	password=pass
else
	echo "Error. Say '$0 database user password or run with defaults '$0'."
	exit
fi

echo "Start creation and initialization of database '${database}' ..."
sudo -u postgres createdb ${database}
sudo -u postgres psql -d ${database} -c "CREATE EXTENSION hstore;"
sudo -u postgres psql -d ${database} -c "CREATE EXTENSION postgis;"
echo "Done."

echo "Start creation of user and initialization of credentials ..."
sudo -u postgres psql -c "CREATE USER \"${user}\" PASSWORD '${password}';"
sudo -u postgres psql -c "GRANT ALL ON DATABASE \"${database}\" TO \"${user}\";"
passphrase="localhost:5432:${database}:${user}:${password}"
if [ ! -e ~/.pgpass ] || [ `less ~/.pgpass | grep -c "$passphrase"` -eq 0 ]
then
	echo "$passphrase" >> ~/.pgpass
	chmod 0600 ~/.pgpass
fi
echo "Done."


echo "Start creation of table bfmap_wayd ..."
sudo -u postgres psql -d ${database} -c "CREATE TABLE bfmap_ways ( \
        gid bigserial,osm_id bigint NOT NULL, \
        class_id integer NOT NULL, \
        source bigint NOT NULL, \
        target bigint NOT NULL, \
        length double precision NOT NULL, \
        reverse double precision NOT NULL, \
        maxspeed_forward integer, \
        maxspeed_backward integer, \
        priority double precision NOT NULL);"

sudo -u postgres psql -d ${database} -c "SELECT AddGeometryColumn('bfmap_ways','geom',4326,'LINESTRING',2);"
sudo -u postgres psql -d bonn -c "ALTER TABLE bfmap_ways OWNER TO \"${user}\";"
echo "Done."
