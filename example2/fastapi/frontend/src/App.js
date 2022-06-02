import './App.css';
import TodoItem from "./TodoItem/TodoItem";
import todoAPI from "./TodoAPI";
import { useEffect, useState } from "react";

const uuidv4 = () => {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(
        /[xy]/g,
        function (c) {
            var r = (Math.random() * 16) | 0,
                v = c == "x" ? r : (r & 0x3) | 0x8;
            return v.toString(16);
        }
    );
};

function App() {

    const [items, setItems] = useState([]);

    function refreshItems() {
        todoAPI.list().then(setItems);
    }

    function handleChange(item) {
        todoAPI.update(item);
    }

    function handleDelete(item) {
        console.log('delete', item);
        todoAPI.delete(item);
        setItems(items.filter((i) => i.id !== item.id));
    }

    function handleAdd() {
        const newItem = {
            id: uuidv4(),
            text: '',
            done: false,
        };
        todoAPI.create(newItem);
        setItems([...items, newItem]);
    }

    useEffect(() => {
        refreshItems();
    }, []);

    return (
        <div className="App">
            <h1>FastAPI Todo List</h1>
            <div className="list-wrapper">
                {items.length === 0 && <p className="empty">This list looks pretty empty, how about adding an item?</p>}
                {items.map(item => (
                    <TodoItem item={item} onChange={handleChange} onDelete={handleDelete} key={item.id}/>
                ))}
            </div>
            <button className="btn-add" onClick={handleAdd}>+</button>
        </div>
    );
}

export default App;
