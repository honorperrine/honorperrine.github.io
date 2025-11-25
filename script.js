const input = document.getElementById("item-input");
const listContainer = document.getElementById("list-container");

// --- CORE FUNCTIONS ---

// Function to add a new list item
function addItem() {
    if (input.value.trim() === '') {
        alert("You must write something!");
        return;
    }

    // 1. Create the new list element
    let li = document.createElement("li");
    li.innerHTML = input.value;

    // 2. Create the close/delete button
    let span = document.createElement("span");
    span.innerHTML = "\u00d7"; // Unicode for 'x'
    span.className = 'close';
    li.appendChild(span);

    // 3. Add the item to the list
    listContainer.appendChild(li);

    // 4. Clear the input and save the list
    input.value = "";
    saveData();
}

// Function to handle clicking on items (toggle check/delete)
listContainer.addEventListener("click", function(e) {
    if (e.target.tagName === "LI") {
        // Toggle the 'checked' class
        e.target.classList.toggle("checked");
        saveData();
    } else if (e.target.tagName === "SPAN") {
        // Remove the parent <li> element
        e.target.parentElement.remove();
        saveData();
    }
}, false);

// --- LOCAL STORAGE FUNCTIONS ---

// Saves the current content of the listContainer to LocalStorage
function saveData() {
    localStorage.setItem("data", listContainer.innerHTML);
}

// Loads and displays the data from LocalStorage when the page loads
function showList() {
    const savedData = localStorage.getItem("data");
    if (savedData) {
        listContainer.innerHTML = savedData;
    }
}

// Load the list immediately when the script runs
showList();

// Allow pressing 'Enter' key to add item
input.addEventListener("keypress", function(event) {
    if (event.key === "Enter") {
        event.preventDefault(); // Prevent default form submission
        addItem();
    }
});
