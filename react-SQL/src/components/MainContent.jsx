// import React, { useEffect, useState } from 'react';
// import Snowlogin from './Snowlogin';
// import { FaBoltLightning } from "react-icons/fa6";
// import SSMSConn from './SSMSlogin';

// const MainContent = () => {
//   const [selectedTable1, setSelectedTable1] = useState('');
//   const [selectedDatabase, setSelectedDatabase] = useState('');
//   const [selectedSchema, setSelectedSchema] = useState('');
//   const [userSelectedTable, setUserSelectedTable] = useState('');

//   // Snowflake data
//   const [sfUsername, setSfUsername] = useState('');
//   const [sfPassword, setSfPassword] = useState('');
//   const [sfAccount, setSfAccount] = useState('');
//   const [sfWarehouse, setSfWarehouse] = useState('');

//   // SSMS data
//   const [ssmsSelectedTable, setSsmsSelectedTable] = useState('');
//   const [ssmsUsername, setSsmsUsername] = useState('');
//   const [ssmsPassword, setSsmsPassword] = useState('');
//   const [ssmsServerName, setSsmsServerName] = useState('');
//   const [ssmsDatabase, setSsmsDatabaseName] = useState('');

//   // Fetch data from localStorage on component mount
//   useEffect(() => {
//     const storedTable1 = localStorage.getItem('selectedTable1');
//     const storedTable = localStorage.getItem('selectedTable');
//     const storedDatabase = localStorage.getItem('selectedDatabase');
//     const storedSchema = localStorage.getItem('selectedSchema');

//     // Snowflake
//     setSfUsername(localStorage.getItem('sfUsername') || '');
//     setSfPassword(localStorage.getItem('sfPassword') || '');
//     setSfAccount(localStorage.getItem('sfAccount') || '');
//     setSfWarehouse(localStorage.getItem('sfWarehouse') || '');

//     // SSMS
//     setSsmsSelectedTable(localStorage.getItem('ssmsSelectedTable') || '');
//     setSsmsUsername(localStorage.getItem('ssmsUsername') || '');
//     setSsmsPassword(localStorage.getItem('ssmsPassword') || '');
//     setSsmsServerName(localStorage.getItem('ssmsServerName') || '');
//     setSsmsDatabaseName(localStorage.getItem('ssmsDatabase') || '');

//     // Handle table, database, and schema
//     if (storedTable) {
//       setSelectedTable1(storedTable);
//       setUserSelectedTable(storedTable); // Use `storedTable` here instead of `storedTable1`
//     } else if (storedTable1) {
//       setSelectedTable1(storedTable1);
//       setUserSelectedTable(storedTable1);
//     }
//     if (storedDatabase) setSelectedDatabase(storedDatabase);
//     if (storedSchema) setSelectedSchema(storedSchema);

//     // Debug logs for verification
//     console.log('Fetched from localStorage:', {
//       storedTable1,
//       storedTable,
//       storedDatabase,
//       storedSchema,
//     });
//   }, []);

//   const handleLoadToSnowflake = async (e) => {
//     e.preventDefault();

//     const bodyData = {
//       selectedTable1,        // The Snowflake table selection
//       selectedDatabase,      // The selected database
//       selectedSchema,        // The selected schema
//       userSelectedTable,     // Assuming userSelectedTable is for user input, if needed
//       sfUsername,            // Snowflake username
//       sfPassword,            // Snowflake password
//       sfAccount,             // Snowflake account
//       sfWarehouse,           // Snowflake warehouse
//       ssmsSelectedTable,     // SSMS selected table
//       ssmsUsername,          // SSMS username
//       ssmsPassword,          // SSMS password
//       ssmsServerName,        // SSMS server name
//       ssmsDatabase,          // SSMS database
//     };

//     console.log('Data to be sent to the API:', bodyData);

//     try {
//       const response = await fetch("http://127.0.0.1:8000/api/load_to_snowflake/", {
//         method: "POST",
//         headers: { 'Content-Type': 'application/json' },
//         body: JSON.stringify(bodyData),
//       });

//       if (!response.ok) {
//         throw new Error('Error loading data to Snowflake.');
//       }

//       const data = await response.json();
//       console.log('API Response:', data);
//     } catch (error) {
//       console.error('Error:', error);
//     }
//   };

//   return (
//     <>
//       <div className="BothLogin">
//         <h1>DataZapp</h1>
//         <SSMSConn />
//         <Snowlogin />
//       </div>
//       <div className="buttonclass">
//         <div className="loadbtn">
//           <button id="ldbtn" onClick={handleLoadToSnowflake}>
//             LOAD
//           </button>
//         </div>
//       </div>
//     </>
//   );
// };

// export default MainContent;



import React, { useEffect, useState } from 'react';
import Snowlogin from './Snowlogin';
import SSMSConn from './SSMSlogin';

const MainContent = () => {
  const [selectedTable1, setSelectedTable1] = useState('');
  const [selectedDatabase, setSelectedDatabase] = useState('');
  const [selectedSchema, setSelectedSchema] = useState('');
  const [userSelectedTable, setUserSelectedTable] = useState('');

  // Snowflake data
  const [sfUsername, setSfUsername] = useState('');
  const [sfPassword, setSfPassword] = useState('');
  const [sfAccount, setSfAccount] = useState('');
  const [sfWarehouse, setSfWarehouse] = useState('');

  // SSMS data
  const [ssmsSelectedTable, setSsmsSelectedTable] = useState('');
  const [ssmsUsername, setSsmsUsername] = useState('');
  const [ssmsPassword, setSsmsPassword] = useState('');
  const [ssmsServerName, setSsmsServerName] = useState('');
  const [ssmsDatabase, setSsmsDatabaseName] = useState('');

  // Fetch data from localStorage on component mount
  useEffect(() => {
    const fetchFromLocalStorage = () => {
      const storedTable1 = localStorage.getItem('selectedTable1') || '';
      const storedTable = localStorage.getItem('selectedTable') || '';
      const storedDatabase = localStorage.getItem('selectedDatabase') || '';
      const storedSchema = localStorage.getItem('selectedSchema') || '';

      setSelectedTable1(storedTable || storedTable1);
      setUserSelectedTable(storedTable || storedTable1);
      setSelectedDatabase(storedDatabase);
      setSelectedSchema(storedSchema);

      // Snowflake
      setSfUsername(localStorage.getItem('sfUsername') || '');
      setSfPassword(localStorage.getItem('sfPassword') || '');
      setSfAccount(localStorage.getItem('sfAccount') || '');
      setSfWarehouse(localStorage.getItem('sfWarehouse') || '');

      // SSMS
      setSsmsSelectedTable(localStorage.getItem('ssmsSelectedTable') || '');
      setSsmsUsername(localStorage.getItem('ssmsUsername') || '');
      setSsmsPassword(localStorage.getItem('ssmsPassword') || '');
      setSsmsServerName(localStorage.getItem('ssmsServerName') || '');
      setSsmsDatabaseName(localStorage.getItem('ssmsDatabase') || '');

      console.log('Fetched from localStorage:', {
        storedTable1,
        storedTable,
        storedDatabase,
        storedSchema,
      });
    };

    fetchFromLocalStorage();
  }, []);

  const handleLoadToSnowflake = async (e) => {
    e.preventDefault();

    // Fetch the latest values from localStorage just before making the API call
    const bodyData = {
      selectedTable1: localStorage.getItem('selectedTable1') || '',
      selectedDatabase: localStorage.getItem('selectedDatabase') || '',
      selectedSchema: localStorage.getItem('selectedSchema') || '',
      userSelectedTable: localStorage.getItem('selectedTable') || '',
      sfUsername: localStorage.getItem('sfUsername') || '',
      sfPassword: localStorage.getItem('sfPassword') || '',
      sfAccount: localStorage.getItem('sfAccount') || '',
      sfWarehouse: localStorage.getItem('sfWarehouse') || '',
      ssmsSelectedTable: localStorage.getItem('ssmsSelectedTable') || '',
      ssmsUsername: localStorage.getItem('ssmsUsername') || '',
      ssmsPassword: localStorage.getItem('ssmsPassword') || '',
      ssmsServerName: localStorage.getItem('ssmsServerName') || '',
      ssmsDatabase: localStorage.getItem('ssmsDatabase') || '',
    };

    console.log('Data to be sent to the API:', bodyData);

    try {
      const response = await fetch("http://127.0.0.1:8000/api/load_to_snowflake/", {
        method: "POST",
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(bodyData),
      });

      if (!response.ok) {
        throw new Error('Error loading data to Snowflake.');
      }

      const data = await response.json();
      console.log('API Response:', data);
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <>
      <div className="BothLogin">
        <h1>DataZapp</h1>
        <SSMSConn />
        <Snowlogin />
      </div>
      <div className="buttonclass">
        <div className="loadbtn">
          <button id="ldbtn" onClick={handleLoadToSnowflake}>
            LOAD
          </button>
        </div>
      </div>
    </>
  );
};

export default MainContent;


