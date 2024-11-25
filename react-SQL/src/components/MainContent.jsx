import React from 'react'
import Snowlogin from './Snowlogin'
import SSMSConn from './SSMSlogin'
const MainContent = () => {
  return (
    <div className='BothLogin'>
      <Snowlogin/>
      <SSMSConn/>
    </div>
  )
}

export default MainContent
