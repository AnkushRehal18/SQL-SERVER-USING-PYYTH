import React, { useState } from 'react';
import axios from "axios";

const SSMSConn = () => {
    const [connectionStatus, setConnectionStatus] = useState("Not connected");
    const [data, setDatabaseData] = useState([]); // State to store database data
    const [showPopup, setShowPopup] = useState(false);
    const [server_name, setServerName] = useState(''); // State for server name
    const [username, setUsername] = useState(''); // State for username
    const [password, setPassword] = useState(''); // State for password

    const handleConnect = async () => {
        console.log("Server Name:", server_name);
        console.log("Username:", username);
        console.log("Password:", password); // Be cautious with logging sensitive information

        // Show connecting status
        setConnectionStatus("Connecting...");
        setShowPopup(true);  // Show popup to indicate the process is ongoing

        const url = "http://127.0.0.1:8000/api/ssms-login/";
        const payload = {
            server_name: server_name,
            username: username,
            password: password,
        };

        try {
            const response = await axios.post(url, payload, {
                headers: {
                    "Content-Type": "application/json",
                },
            });

            console.log("Success:", response.data);

            // Check if the response contains success information
            if (response.status === 200 && response.data) {
                setConnectionStatus("Connected");  // Update connection status on success
                console.log(connectionStatus, 'if block')
                setDatabaseData(response.data); // Store database response
            } else {
                // setConnectionStatus("Not connected"); // Update if not successful'
                console.log(connectionStatus, 'else block')
            }
        } catch (error) {
            console.error("Error:", error);

        }

        // After the request finishes, close the popup
        setShowPopup(false);
    };

    const closePopup = () => {
        setShowPopup(false);
    };

    return (
        <>

            <div className="container">
                <h2 className="text-3xl text-blue-700">Connect To SSMS</h2>
                <br></br>
                <div className="form-group">
                    <label className='mt-10 text-2xl text-blue-500'>Server Name</label>
                    <br></br>
                    <input className='w-[250px] h-[40px] p-2 border rounded-md'
                        type="text"
                        id="server-name"
                        name="server-name"
                        placeholder="Enter server name"
                        value={server_name}
                        onChange={(e) => setServerName(e.target.value)} // Update state on input change
                    />
                </div>

                <div>
                    <label className='mt-10 text-2xl text-blue-500'>User Name</label>
                    <br></br>
                    <input className='w-[250px] h-[40px] p-2 border rounded-md'
                        type="text"
                        id="username"
                        name="username"
                        placeholder="Enter username"
                        value={username}
                        onChange={(e) => setUsername(e.target.value)} // Update state on input change
                    />
                </div>

                <div>
                    <label className='mt-10 text-2xl text-blue-500'>Password</label>
                    <br></br>
                    <input className='w-[250px] h-[40px] p-2 border rounded-md'
                        type="password"
                        id="password"
                        name="password"
                        placeholder="Enter password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)} // Update state on input change
                    />
                </div>
                <button id="Connect" className='button bg-blue-500 h-9 w-[270px] mt-[20px] text-xl font-serif text-white hover:bg-sky-700 border rounded-lg'
                    onClick={handleConnect}>
                    Connect to SQL Server
                </button>
                <br></br><br></br>

                <div className="flex items-center space-x-4">
                    <span id="connection-status" className='text-xl text-blue-500'>Connection Status:</span>
                    {connectionStatus === "Connected" ? (
                        <p className="text-green-600 text-xl">Connected</p>
                    ) : (
                        <p className="text-red-600 text-xl">Not Connected</p>
                    )}
                </div>

            </div>
        </>
    );
};

export default SSMSConn;

