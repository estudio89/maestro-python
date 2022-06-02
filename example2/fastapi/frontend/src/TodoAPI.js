const api = {
    list: async function () {
        const response = await fetch("/api/todo/");
        return await response.json();
    },
    create: async function (item) {
        console.log('create', item);
        await fetch("/api/todo/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(item),
        });
    },
    update: async function (item) {
        await fetch(`/api/todo/${item.id}/`, {
            method: "PUT",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify(item),
        });
    },
    delete: async function (item) {
        await fetch(`/api/todo/${item.id}/`, {
            method: "DELETE",
        });
    },
};

export default api;