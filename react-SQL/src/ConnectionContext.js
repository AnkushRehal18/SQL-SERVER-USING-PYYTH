import React, { createContext, useState } from "react";

// Create a Context
export const ConnectionContext = createContext();

// Create a Provider Component
export const ConnectionProvider = ({ children }) => {
  const [ssmsConnection, setSSMSConnection] = useState(null); // Store SSMS connection status/details
  const [snowflakeConnection, setSnowflakeConnection] = useState(null); // Store Snowflake connection status/details

  return (
    <ConnectionContext.Provider
      value={{
        ssmsConnection,
        setSSMSConnection,
        snowflakeConnection,
        setSnowflakeConnection,
      }}
    >
      {children}
    </ConnectionContext.Provider>
  );
};
