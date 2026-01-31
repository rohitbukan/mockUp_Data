let columns = [];
let tableData = [];

function uploadSQL() {
  const file = document.getElementById("sqlFile").files[0];
  if (!file) return;

  const formData = new FormData();
  formData.append("file", file);

  fetch("/upload-sql", { method: "POST", body: formData })
    .then(res => res.json())
    .then(data => {
      if (data.ddl) {
        document.getElementById("ddlInput").value = data.ddl;
      } else {
        alert(data.message || "Failed to upload DDL file");
      }
    })
    .catch(err => alert("Failed to upload DDL file"));
}

function generate() {
  const ddlText = document.getElementById("ddlInput").value;
  const rows = document.getElementById("rows").value;

  fetch("/generate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ddl: ddlText, rows: rows })
  })
  .then(res => res.json())
  .then(data => {
    if (!data.success) {
      alert(data.message || "Failed to generate data");
      return;
    }

    columns = data.columns;
    tableData = data.data;

    if (columns.length === 0) {
      alert("No columns detected");
      return;
    }

    renderTable();
    document.getElementById("preview").style.display = "block";
  })
  .catch(err => {
    console.error(err);
    alert("Server error while generating preview");
  });
}

function renderTable() {
  const table = document.getElementById("table");
  table.innerHTML = "";

  if (!columns.length || !tableData.length) {
    table.innerHTML = "<tr><td>No data to display</td></tr>";
    return;
  }

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  columns.forEach(col => {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  });

  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");

  tableData.forEach((row, rowIndex) => {
    const tr = document.createElement("tr");

    columns.forEach(col => {
      const td = document.createElement("td");
      td.contentEditable = true;
      td.textContent = row[col] ?? "";
      td.oninput = () => tableData[rowIndex][col] = td.textContent;
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
}

function addRow() {
  const r = {};
  columns.forEach(c => r[c] = "");
  tableData.push(r);
  renderTable();
}

function downloadCSV() {
  fetch("/csv", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ columns: columns, data: tableData })
  })
  .then(res => res.blob())
  .then(blob => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "dummy_data.csv";
    a.click();
  })
  .catch(() => alert("Failed to generate CSV"));
}

function generateInsertSQL() {
  fetch("/insert-sql", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ddl: document.getElementById("ddlInput").value, columns, data: tableData })
  })
  .then(res => res.blob())
  .then(blob => {
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = "insert_statements.sql";
    a.click();
  })
  .catch(() => alert("Failed to generate INSERT SQL"));
}
