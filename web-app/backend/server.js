const express = require('express');
const bodyParser = require('body-parser');
const { exec } = require('child_process');

const app = express();
const port = 3000;

app.use(bodyParser.json());

app.post('/run-job', (req, res) => {
    const {
        kafkaBootstrapServers,
        kafkaTopics,
        lakehouseHost,
        lakehousePort,
        lakehouseDb,
        lakehouseUser,
        lakehousePassword
    } = req.body;

    const command = `
        spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 --master local[*] main.py
        --kafka-bootstrap-servers ${kafkaBootstrapServers}
        --kafka-topic ${kafkaTopics}
        --lakehouse-host ${lakehouseHost}
        --lakehouse-port ${lakehousePort}
        --lakehouse-db ${lakehouseDb}
        --lakehouse-user ${lakehouseUser}
        --lakehouse-password ${lakehousePassword}
    `;

    exec(command, (error, stdout, stderr) => {
        if (error) {
            console.error(`Error: ${error.message}`);
            return res.status(500).json({ message: 'Failed to run job.' });
        }
        if (stderr) {
            console.error(`stderr: ${stderr}`);
            return res.status(500).json({ message: 'Failed to run job.' });
        }
        console.log(`stdout: ${stdout}`);
        res.status(200).json({ message: 'Job ran successfully!' });
    });
});

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}/`);
});