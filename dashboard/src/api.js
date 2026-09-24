const API_URL = "http://localhost:8000";


async function request(path) {

  const response = await fetch(
    `${API_URL}${path}`
  );

  if (!response.ok) {
    throw new Error(
      `API request failed: ${response.status}`
    );
  }

  return response.json();
}


export function getWardOverview() {
  return request(
    "/api/ward/overview"
  );
}


export function getPatients() {
  return request(
    "/api/patients"
  );
}


export function getPatientLatest(patientId) {
  return request(
    `/api/patients/${patientId}/latest`
  );
}


export function getPatientHistory(
  patientId,
  limit = 30
) {

  return request(
    `/api/patients/${patientId}/history?limit=${limit}`
  );
}


export function getAlerts(limit = 10) {
  return request(
    `/api/alerts?limit=${limit}`
  );
}


export function getReports() {
  return request(
    "/api/reports/daily"
  );
}


export function getDailyReport(day) {
  return request(
    `/api/reports/daily/${day}`
  );
}

export function getHealth() {
  return request("/health");
}