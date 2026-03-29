
let prescriptions = [];

function getColor(t){
  if(t=="Emergency") return "red";
  if(t=="Inpatient") return "blue";
  if(t=="Outpatient") return "green";
  if(t=="Discharge") return "purple";
  return "gray";
}

function savePrescription(){
  let data={
    drug:drug.value,
    patient:patient.value,
    file:file.value,
    type:type.value
  };
  prescriptions.unshift(data);
  renderTable();
}

function renderTable(){
  let f = document.getElementById("filterType").value;
  let t = document.getElementById("table");
  t.innerHTML="<tr><th>Drug</th><th>Patient</th><th>File</th><th>Type</th></tr>";

  prescriptions.filter(x=>!f||x.type==f).forEach(r=>{
    t.innerHTML+=`
    <tr>
      <td>${r.drug}</td>
      <td>${r.patient}</td>
      <td>${r.file}</td>
      <td><span class="badge" style="background:${getColor(r.type)}">${r.type||"-"}</span></td>
    </tr>`;
  });
}

/* RECEIVE */

let receive=[];

function openReceive(){
  modal.classList.remove("hidden");
  modalContent.innerHTML=`
  <div class="split">
    <div class="right">
      <h3>Receive</h3>
      <input id="rDrug" placeholder="Drug">
      <input id="rBox" placeholder="Boxes">
      <input id="rUnit" placeholder="Units">
      <button onclick="addReceive()">Done</button>
    </div>
    <div class="left">
      <table id="rTable"></table>
      <button onclick="alert('Receive Done')">Submit</button>
    </div>
  </div>`;
}

function addReceive(){
  receive.push({d:rDrug.value,b:rBox.value,u:rUnit.value});
  renderReceive();
}

function renderReceive(){
  let t=document.getElementById("rTable");
  t.innerHTML="";
  receive.forEach(x=>{
    t.innerHTML+=`<tr><td>${x.d}</td><td>${x.b}</td><td>${x.u}</td></tr>`;
  });
}

/* TRANSFER */

let transfer=[];

function openTransfer(){
  modal.classList.remove("hidden");
  modalContent.innerHTML=`
  <div class="split">
    <div class="right">
      <h3>Transfer</h3>
      <input id="tDrug" placeholder="Drug">
      <input id="tBox" placeholder="Boxes">
      <input id="tUnit" placeholder="Units">
      <button onclick="addTransfer()">Done</button>
    </div>
    <div class="left">
      <table id="tTable"></table>
      <button onclick="alert('Transfer Done')">Submit</button>
    </div>
  </div>`;
}

function addTransfer(){
  transfer.push({d:tDrug.value,b:tBox.value,u:tUnit.value});
  renderTransfer();
}

function renderTransfer(){
  let t=document.getElementById("tTable");
  t.innerHTML="";
  transfer.forEach(x=>{
    t.innerHTML+=`<tr><td>${x.d}</td><td>${x.b}</td><td>${x.u}</td></tr>`;
  });
}

/* CLOSE MODAL */

document.addEventListener("click",e=>{
  if(e.target.id==="modal"){
    modal.classList.add("hidden");
  }
});
