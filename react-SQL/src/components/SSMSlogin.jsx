import React, { useEffect, useState } from 'react';
import axios from "axios";
import { ImDatabase } from "react-icons/im";
import PulseLoader from "react-spinners/PulseLoader";

const SSMSConn = () => {
    const [connectionStatus, setConnectionStatus] = useState("Not connected");
    const [databaseData, setDatabaseData] = useState([]); // State to store database data
    const [tableData, setTableData] = useState([]); // State to store tables of the selected database
    const [serverName, setServerName] = useState('DESKTOP-4OQB5NR'); // State for server name
    const [username, setUsername] = useState('gurjotsingh'); // State for username
    const [password, setPassword] = useState('new123'); // State for password
    const [selectedDatabase, setSelectedDatabase] = useState(''); // State for selected database
    const [selectedTable, setSelectedTable] = useState(''); // State for selected table
    const [loading, setLoading] = useState(false); // State to manage loader visibility
    const [tableLoading, setTableLoading] = useState(false); // State for table loading animation

    // Update localStorage whenever selectedTable changes
    useEffect(() => {
        if (selectedTable) {
            localStorage.setItem('ssmsSelectedTable', selectedTable);
            localStorage.setItem('ssmsUsername', username);
            localStorage.setItem('ssmsPassword', password);
            localStorage.setItem('ssmsServerName', serverName);
            localStorage.setItem('ssmsDatabase', selectedDatabase);
        }
    }, [selectedTable]);

    // Function to handle connection and fetch databases
    const handleConnect = async () => {
        console.log("handleConnect called ----");
        setConnectionStatus("Connecting...");
        setLoading(true); // Show loader
        const url = "http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/";
        const payload = {
            server_name: serverName,
            username: username,
            password: password,
        };

        try {
            const response = await axios.post(url, payload, {
                headers: {
                    "Content-Type": "application/json",
                },
            });

            if (response.status === 200 && response.data.success) {
                setConnectionStatus("Connected");
                setDatabaseData(response.data.databases || []);
                setSelectedDatabase('');
                setTableData([]);
            } else {
                setConnectionStatus("Not connected");
                setDatabaseData([]);
            }
        } catch (error) {
            console.error("Error:", error);
            setConnectionStatus("Not connected");
            setDatabaseData([]);
        } finally {
            setLoading(false); // Hide loader
        }
    };

    // Function to fetch tables of the selected database
    const handleDatabaseSelection = async (database) => {
        console.log("handleDatabaseSelection called ----");
        setSelectedDatabase(database);
        setSelectedTable('');
        setTableLoading(true); // Show loader for fetching tables

        const url = "http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/";
        const payload = {
            server_name: serverName,
            username: username,
            password: password,
            selected_database: database,
        };

        try {
            const response = await axios.post(url, payload, {
                headers: {
                    "Content-Type": "application/json",
                },
            });
            if (response.status === 200) {
                setTableData(response.data.tables || []);
            }
        } catch (error) {
            console.error("Error fetching tables:", error);
        } finally {
            setTableLoading(false); // Hide loader
        }
    };

    // Function to handle table selection
    const handleTableSelection = (table) => {
        setSelectedTable(table);
        console.log("Selected Table:", table);
    };

    return (
        <div className="container">
            <h2><ImDatabase /> Connect To SSMS</h2>
            <br />

            {/* Input for server name */}
            <div className="form-group">
                <label className='ServerName'>Server Name</label>
                <br />
                <input
                    className='input-servername'
                    type="text"
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
                Connect to SSMS
            </button>

            {/* Loader for connection */}
            {loading && (
                <div className="loader-container">
                    <PulseLoader color="#2e18c3" />
                </div>
            )}

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
                <div className="database-select-container">
                <h3 className='fs-5'>Select Database:</h3>
                <select
                    value={selectedDatabase}
                    onChange={(e) => handleDatabaseSelection(e.target.value)}
                    className="database-select"
                >
                    <option value="">-- Select Database --</option>
                    {databaseData.map((db, index) => (
                        <option key={index} value={db}>{db}</option>
                    ))}
                </select>
            </div>
            
            )}

            {/* Loader for fetching tables */}
            {tableLoading && (
                <div className="loader-container ">
                   <PulseLoader color="#2e18c3" />
                </div>
            )}

            {/* Render table selection */}
            {selectedDatabase && (
                <div>
                    <h3 className='fs-5'>Select Table:</h3>
                    <select
                        value={selectedTable}
                        onChange={(e) => handleTableSelection(e.target.value)}
                        className="table-select"
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
                <div>
                    <h3>Selected Table: {selectedTable}</h3>
                </div>
            )}
        </div>
    );
};

export default SSMSConn;
