import React, { useState } from 'react';

const SnowflakeConnectForm = () => {
  // Define state variables for each input field
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [account, setAccount] = useState('');
  const [warehouse, setWarehouse] = useState('');
  const [database, setDatabase] = useState('');
  const [schema, setSchema] = useState('');
  const [connectionStatus, setConnectionStatus] = useState(''); // State for connection status

  // Function to handle form submission
  const handleConnect = async (e) => {
    e.preventDefault();

    try {
      const response = await fetch('http://127.0.0.1:8000/api/snowflake-login/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password, account, warehouse, database, schema }),
      });

      const data = await response.json();
      alert(data.message || "Login Failed")
      // Update connection status based on response
      if (response.ok) {
        setConnectionStatus('Connected');
      } else {
        setConnectionStatus('Not Connected');
      }
    } catch (error) {
      setConnectionStatus('Not Connected');
      console.error('Error connecting to Snowflake:', error);
    }
  };

  return (
    <div className="w-[1000px] ml-[330px]">
      <h2 className="text-3xl text-blue-700">Connect to Snowflake</h2>
      <br />
      <div>
        <label className="mt-10 text-2xl text-blue-500">Username</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your Username"
          type="text"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
        />
      </div>
      <div>
        <label className="mt-10 text-2xl text-blue-500">Password</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your password"
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>
      <div>
        <label className="mt-10 text-2xl text-blue-500">Account</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your account"
          type="text"
          value={account}
          onChange={(e) => setAccount(e.target.value)}
        />
      </div>
      <div>
        <label className="mt-10 text-2xl text-blue-500">Warehouse</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your Warehouse"
          type="text"
          value={warehouse}
          onChange={(e) => setWarehouse(e.target.value)}
        />
      </div>
      <div>
        <label className="mt-10 text-2xl text-blue-500">Database</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your Database"
          type="text"
          value={database}
          onChange={(e) => setDatabase(e.target.value)}
        />
      </div>
      <div>
        <label className="mt-10 text-2xl text-blue-500">Schema</label>
        <br />
        <input
          className="w-[250px] h-[40px] p-2 border rounded-md"
          placeholder="Enter your Schema"
          type="text"
          value={schema}
          onChange={(e) => setSchema(e.target.value)}
        />
      </div>
      <button
        className="bg-blue-500 h-9 w-[270px] mt-[20px] text-xl font-serif text-white hover:bg-sky-700 border rounded-lg"
        onClick={handleConnect}
      >
        Connect
      </button>

      {/* Connection Status */}
      <div className="flex items-center space-x-4 mt-5">
        <span id="connection-status" className="text-xl text-blue-500">
          Connection Status:
        </span>
        {connectionStatus === 'Connected' ? (
          <p className="text-green-600 text-xl">Connected</p>
        ) :(
          <p className="text-red-600 text-xl">Not Connected</p>
        )}
      </div>
    </div>
  );
};

export default SnowflakeConnectForm;
