
finally:
    # Close the connection
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Connection closed.")
