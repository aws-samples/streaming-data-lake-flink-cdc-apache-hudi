
yum install -y postgresql
yum install -y git
yum update -y
cd /home/ec2-user
DIR="aws-database-migration-samples"
if [ ! -d "$DIR" ]; then
  git clone https://github.com/aws-samples/aws-database-migration-samples.git
fi
cd aws-database-migration-samples/PostgreSQL/sampledb/v1/
export PGPASSWORD=admin123
nohup psql --host=${!ENDPOINT} --port=5432 --dbname=sportstickets --username=adminuser -f install-postgresql.sql