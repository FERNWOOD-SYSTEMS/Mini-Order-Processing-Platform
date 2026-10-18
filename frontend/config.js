// Where the browser should send API requests.
//
//   Local development : "http://localhost:8001"
//   Behind one ALB    : "" (empty string = same origin, the ALB routes /orders* to the API)
//
// This file is separate from app.js on purpose. In Part 2 you will copy it into the image,
// and in Part 7 you can replace it per environment without touching the application code.
window.API_BASE_URL = "http://localhost:8001";
