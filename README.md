# CaMeLS server
### Visit the [repo with the latest CaMeLS implementation](https://github.com/ISG-Siegen/camels) for more resources.

---

## Exemplary implementations for all required files are found in the CaMeLS release through the link above.

## Instructions
1. The file `server_database_identifier.py` has to be implemented.
It contains all references algorithms, tasks, metrics, meta-learners, and metadata.
This file is automatically distributed and synced with connecting clients to ensure consistency.
2. Implement wrapper files for supported libraries and the meta-learners and distribute these files to clients.
3. Start the server with `flask run server.py`.
You can specify the host address of the server among other settings with command-line parameters.
4. Make sure that the server address is static and make it available to clients.
5. Let a client connect to the server and run the `populate_database` routine to create the database.
6. Clients can now use the server.
