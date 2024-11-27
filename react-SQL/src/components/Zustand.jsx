// useStore.js
import create from 'zustand';

const useStore = create((set) => ({
    connection: null,
    setConnection: (conn) => set({ connection: conn }),
}));

export default useStore;