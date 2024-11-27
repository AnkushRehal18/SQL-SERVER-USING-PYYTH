import React from 'react'
import Snowlogin from './Snowlogin'
import { FaBoltLightning } from "react-icons/fa6";
import SSMSConn from './SSMSlogin'
import { BrowserRouter as Router , Route , Routes } from 'react-router-dom'
const MainContent = () => {
  return (
    <>
    <div className='BothLogin'>
      <h1>DataZapp</h1>
      <SSMSConn/>
      <Snowlogin/>
    </div>
    <div className='buttonclass'>
    <div className="loadbtn">
      <button id='ldbtn'>LOAD</button>
    </div>
    </div>
    
    
    </>
    
  )
}

export default MainContent
