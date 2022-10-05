@echo off
echo "==================     Help for psql   ========================="
echo "\\l       : List of databases
echo "\\dt		: Describe the current database"
echo "\\d [table]	: Describe a table"
echo "\\c		: Connect to a database"
echo "\\h		: help with SQL commands"
echo "\\?		: help with psql commands"
echo "\\q		: quit"
echo "=================================================================="
@echo on
docker exec -it postgres psql -U docker -d rtjvm
