const { Client } = require('pg');

exports.handler = async (event, context, cb) => {
    var query_cmd = "select dms_sample.generateticketactivity(500);"

    var query_alter = 'ALTER TABLE dms_sample.sporting_event_ticket REPLICA IDENTITY FULL;'

    const client = new Client({
        user: "adminuser",
        password: process.env.PASSWORD,
        host: process.env.HOST,
        database: "sportstickets",
        port: 5432
    });

    await client.connect();  // Your other interactions with RDS...

    const resultAlter = await client.query(query_alter);
    const result = await client.query(query_cmd);
    const resultString = JSON.stringify(result);

    client.end();

        const response = {
        "statusCode":200,
        "body":resultString
        };

        return response;

  };
