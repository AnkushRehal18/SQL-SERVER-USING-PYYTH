import React, { useEffect, useState } from 'react';
import axios from "axios";
import { ImDatabase } from "react-icons/im";

const SSMSConn = () => {
    const [connectionStatus, setConnectionStatus] = useState("Not connected");
    const [databaseData, setDatabaseData] = useState([]); // State to store database data
    const [tableData, setTableData] = useState([]); // State to store tables of the selected database
    const [serverName, setServerName] = useState(''); // State for server name
    const [username, setUsername] = useState(''); // State for username
    const [password, setPassword] = useState(''); // State for password
    const [selectedDatabase, setSelectedDatabase] = useState(''); // State for selected database
    const [selectedTable, setSelectedTable] = useState(''); // State for selected table

     // Update localStorage whenever selectedTable changes
     useEffect(() => {
        if (selectedTable) {
            localStorage.setItem('ssmsSelectedTable', selectedTable);
            localStorage.setItem('ssmsUsername', username);
            localStorage.setItem('ssmsPassword', password);
            localStorage.setItem('ssmsServerName', serverName);
            localStorage.setItem('ssmsDatabase',selectedDatabase);
        }
    }, [selectedTable]);  // This runs every time selectedTable changes
    

    // 1. handleConnect for coonnection
    // 2. handleDatabaseSelection
    // Function to handle connection and fetch databases

    const handleConnect = async () => {
        console.log("handleConnect called ----");
        console.log("Server Name:", serverName);
        console.log("Username:", username);
        console.log("Password:", password); // Be cautious with logging sensitive information
        console.log("Database", selectedDatabase);

        // Show connecting status
        setConnectionStatus("Connecting...");

        const url = "http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/";
        const payload = {
            server_name: serverName,
            username: username,
            password: password,
        };

        try {
            console.log("Sending payload to backend:", payload);
            const response = await axios.post(url, payload, {
                headers: {
                    "Content-Type": "application/json",
                },
            });

            console.log("Success:", response.data);

            // Check if the response contains success information
            if (response.status === 200 && response.data.success) {
                setConnectionStatus("Connected");  // Update connection status on success
                setDatabaseData(response.data.databases || []); // Store the list of databases or an empty array
                setSelectedDatabase('');  // Reset selected database
                setTableData([]);  // Reset table data
                console.log("Incoming data:", response.data.databases);
            } else {
                setConnectionStatus("Not connected");
                console.log("Response data:", response.data);
                setDatabaseData([]); // Ensure data is always an array
            }
        } catch (error) {
            console.error("Error:", error);
            setConnectionStatus("Not connected");
            setDatabaseData([]); // Ensure data is always an array
        }
        console.log("handleConnect end ----");
    };

    // Function to fetch tables of the selected database
    const handleDatabaseSelection = async (database) => {
        console.log("handleDatabaseSelection called ----");
        setSelectedDatabase(database);
        setSelectedTable(''); // Reset selected table

        const url = "http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/";
        const payload = {
            server_name: serverName,
            username: username,
            password: password,
            selected_database: database, // Send selected database to fetch tables
        };

        try {
            
            const response = await axios.post(url, payload, {
                headers: {
                    "Content-Type": "application/json",
                },
            });
            // console.log("Response received from backend:", response);
            console.log("Tables Response:", response.data);
            
            if (response.status === 200) {
                setTableData(response.data.tables || []); // Store tables for the selected database
            }
        } catch (error) {
            console.error("Error fetching tables:", error);
        }
        console.log("handleDatabaseSelection end ----");
    };

    // Function to handle table selection
    const handleTableSelection = (table) => {
        console.log("handleTableSelection called ----");
        setSelectedTable(table);
        console.log("Selected Table:", table);
        console.log("handleTableSelection end ----");
        // Optionally, you can now make another API call to fetch the data from the selected table
    };

    return (
        <>
            <div className="container">
                <h2><ImDatabase />Connect To SSMS</h2>
                <br />

                {/* Input for server name */}
                <div className="form-group">
                    <label className='ServerName'>Server Name</label>
                    <br />
                    <input
                        className='input-servername'
                        type="text"
                        id="server-name"
                        name="server-name"
                        placeholder="Enter server name"
                        value={serverName}
                        onChange={(e) => setServerName(e.target.value)}
                    />
                </div>

                {/* Input for username */}
                <div>
                    <label className='username'>User Name</label>
                    <br />
                    <input
                        className='usernameinput'
                        type="text"
                        id="username"
                        name="username"
                        placeholder="Enter username"
                        value={username}
                        onChange={(e) => setUsername(e.target.value)}
                    />
                </div>

                {/* Input for password */}
                <div>
                    <label className='Password'>Password</label>
                    <br />
                    <input
                        className='inputPassword'
                        type="password"
                        id="password"
                        name="password"
                        placeholder="Enter password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                    />
                </div>

                {/* Connect button */}
                <button
                    id="Connect"
                    className='connectbtn'
                    onClick={handleConnect}
                >
                    Connect to SQL Server
                </button>

                <br /><br />

                {/* Connection status display */}
                <div className="ConnectionStatus">
                    <span id="connection-status" className='ConnectionStaus2 text-primary fs-5'>Connection Status:</span>
                    {connectionStatus === "Connected" ? (
                        <p className="text-green Connected">Connected</p>
                    ) : (
                        <p className="text-red Not-Connected">Not Connected</p>
                    )}
                </div>

                {/* Render database selection */}
                {connectionStatus === "Connected" && (
                    <div>
                        <h3 className="SelectDatabase">Select Database:</h3>
                        <select
                            value={selectedDatabase}
                            onChange={(e) => handleDatabaseSelection(e.target.value)}
                            className="w-[250px] h-[40px] p-2 border rounded-md mt-2"
                        >
                            <option value="">-- Select Database --</option>
                            {databaseData.map((db, index) => (
                                <option key={index} value={db}>{db}</option>
                            ))}
                        </select>
                    </div>
                )}

                {/* Render table selection after a database is selected */}
                {selectedDatabase && (
                    <div>
                        <h3 className="SelectTable">Select Table:</h3>
                        <select
                            value={selectedTable}
                            onChange={(e) => handleTableSelection(e.target.value)}
                            className="SelectTable1"
                        >
                            <option value="">-- Select Table --</option>
                            {tableData.map((table, index) => (
                                <option key={index} value={table}>{table}</option>
                            ))}
                        </select>
                    </div>
                )}

                {/* Display selected table */}
                {selectedTable && (
                    <div className="SelectedTable2">
                        <h3 className="SelectedTable3">Selected Table: {selectedTable}</h3>
                    </div>
                )}
            </div>
        </>
    );
};

export default SSMSConn;
