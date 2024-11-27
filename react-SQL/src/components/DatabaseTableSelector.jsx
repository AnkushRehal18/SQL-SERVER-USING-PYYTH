// import React, { useState } from "react";

// function DatabaseSelector({ databases = [], error, loading }) {
//   const [selectedDatabase, setSelectedDatabase] = useState(""); // Selected database
//   const [tables, setTables] = useState([]); // Tables for the selected database
//   const [selectedTable, setSelectedTable] = useState(""); // Selected table

//   // Fetch the list of tables when a database is selected
//   const handleDatabaseChange = async (event) => {
//     const selectedDb = event.target.value;
//     setSelectedDatabase(selectedDb);

//     setTables([]); // Clear tables when changing database
//     setSelectedTable(""); // Reset selected table

//     if (selectedDb) {
//       try {
//         const response = await axios.get(
//           `http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/?database=${selectedDb}`
//         );
//         if (response.data.tables) {
//           setTables(response.data.tables); // Ensure response contains tables
//         } else {
//           throw new Error("Unexpected API response structure");
//         }
//         console.log(`Tables for database ${selectedDb} fetched successfully:`, response.data);
//       } catch (err) {
//         console.error("Error fetching tables:", err);
//         setTables([]); // Reset tables if API call fails
//       }
//     }
//   };

//   const handleTableChange = (event) => {
//     setSelectedTable(event.target.value);
//   };

//   return (
//     <div className="database-selector">
//       {loading && <p>Loading...</p>}
//       {error && <p className="error">Error: {error}</p>}

//       {/* Database Dropdown */}
//       <div className="form-group">
//         <label htmlFor="databases">Database:</label>
//         <select
//           id="databases"
//           className="dropdown"
//           value={selectedDatabase}
//           onChange={handleDatabaseChange}
//         >
//           <option value="" disabled>
//             Select a database
//           </option>
//           {databases && databases.length > 0 ? (
//             databases.map((db, index) => (
//               <option key={index} value={db}>
//                 {db}
//               </option>
//             ))
//           ) : (
//             <option disabled>No databases available</option>
//           )}
//         </select>
//       </div>

//       {/* Tables Dropdown */}
//       <div className="form-group">
//         <label htmlFor="tables">Tables:</label>
//         <select
//           id="tables"
//           className="dropdown"
//           value={selectedTable}
//           onChange={handleTableChange}
//           disabled={!selectedDatabase} // Disable table selection if no database is selected
//         >
//           <option value="" disabled>
//             {tables.length > 0 ? "Select a table" : "No tables available"}
//           </option>
//           {tables.map((table, index) => (
//             <option key={index} value={table}>
//               {table}
//             </option>
//           ))}
//         </select>
//       </div>

//       {/* Display Selected Database and Table */}
//       <div className="selected-info">
//         <p>Selected Database: {selectedDatabase || "None"}</p>
//         <p>Selected Table: {selectedTable || "None"}</p>
//       </div>
//     </div>
//   );
// }

// export default DatabaseSelector;



import React, { useState } from "react";
import axios from "axios";

function DatabaseSelector({ databases = [], error, loading }) {
  const [selectedDatabase, setSelectedDatabase] = useState(""); // Selected database
  const [tables, setTables] = useState([]); // Tables for the selected database
  const [selectedTable, setSelectedTable] = useState(""); // Selected table

  // Fetch the list of tables when a database is selected
  const handleDatabaseChange = async (event) => {
    const selectedDb = event.target.value;
    setSelectedDatabase(selectedDb);

    setTables([]); // Clear tables when changing database
    setSelectedTable(""); // Reset selected table

    if (selectedDb) {
      try {
        const response = await axios.post(
          `http://127.0.0.1:8000/api/SSMS_Login_And_FetchData/?database=${selectedDb}`
        );
        if (response.data && Array.isArray(response.data.tables)) {
          setTables(response.data.tables); // Ensure response contains an array of tables
        } else {
          console.error("Error: Invalid response structure");
          setTables([]); // If response is invalid, reset tables
        }
        console.log(`Tables for database ${selectedDb} fetched successfully:`, response.data);
      } catch (err) {
        console.error("Error fetching tables:", err);
        setTables([]); // Reset tables if API call fails
      }
    }
  };

  const handleTableChange = (event) => {
    setSelectedTable(event.target.value);
  };

  return (
    <div className="database-selector">
      {loading && <p>Loading...</p>}
      {error && <p className="error">Error: {error}</p>}

      {/* Database Dropdown */}
      <div className="form-group">
        <label htmlFor="databases">Database:</label>
        <select
          id="databases"
          className="dropdown"
          value={selectedDatabase}
          onChange={handleDatabaseChange}
        >
          <option value="" disabled>
            Select a database
          </option>
          {databases && databases.length > 0 ? (
            databases.map((db, index) => (
              <option key={index} value={db}>
                {db}
              </option>
            ))
          ) : (
            <option disabled>No databases available</option>
          )}
        </select>
      </div>

      {/* Tables Dropdown */}
      <div className="form-group">
        <label htmlFor="tables">Tables:</label>
        <select
          id="tables"
          className="dropdown"
          value={selectedTable}
          onChange={handleTableChange}
          disabled={!selectedDatabase} // Disable table selection if no database is selected
        >
          <option value="" disabled>
            {tables.length > 0 ? "Select a table" : "No tables available"}
          </option>
          {tables && tables.length > 0 ? (
            tables.map((table, index) => (
              <option key={index} value={table}>
                {table}
              </option>
            ))
          ) : (
            <option disabled>No tables available</option>
          )}
        </select>
      </div>

      {/* Display Selected Database and Table */}
      <div className="selected-info">
        <p>Selected Database: {selectedDatabase || "None"}</p>
        <p>Selected Table: {selectedTable || "None"}</p>
      </div>
    </div>
  );
}

export default DatabaseSelector;
