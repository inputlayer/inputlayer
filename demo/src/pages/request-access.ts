/**
 * Email capture page served at GET /demo/request-access
 */
export function requestAccessPage(defaultKg: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Request Demo Access - InputLayer</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      background: #09090b;
      color: #fafafa;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
    }
    .container {
      width: 100%;
      max-width: 420px;
      padding: 24px;
    }
    .logo {
      font-size: 18px;
      font-weight: 700;
      text-align: center;
      margin-bottom: 8px;
    }
    .subtitle {
      text-align: center;
      font-size: 13px;
      color: #a1a1aa;
      margin-bottom: 32px;
    }
    .card {
      background: #18181b;
      border: 1px solid #27272a;
      border-radius: 12px;
      padding: 24px;
    }
    .card-header {
      display: flex;
      align-items: center;
      gap: 12px;
      background: #1c1c20;
      border-radius: 8px;
      padding: 12px 16px;
      margin-bottom: 20px;
    }
    .card-header-icon {
      width: 20px;
      height: 20px;
      color: #a1a1aa;
    }
    .card-header-text h2 {
      font-size: 14px;
      font-weight: 500;
    }
    .card-header-text p {
      font-size: 11px;
      color: #71717a;
    }
    .field {
      margin-bottom: 16px;
    }
    .field label {
      display: block;
      font-size: 11px;
      font-weight: 500;
      color: #a1a1aa;
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .field input, .field select {
      width: 100%;
      height: 40px;
      padding: 0 12px;
      background: #09090b;
      border: 1px solid #27272a;
      border-radius: 6px;
      color: #fafafa;
      font-size: 14px;
      outline: none;
      transition: border-color 0.15s;
    }
    .field input:focus, .field select:focus {
      border-color: #2563eb;
    }
    .field input::placeholder {
      color: #52525b;
    }
    .btn {
      width: 100%;
      height: 40px;
      background: #2563eb;
      color: #fff;
      border: none;
      border-radius: 6px;
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s;
      margin-top: 4px;
    }
    .btn:hover { background: #1d4ed8; }
    .btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .error {
      background: rgba(239, 68, 68, 0.1);
      color: #ef4444;
      padding: 8px 12px;
      border-radius: 6px;
      font-size: 13px;
      margin-bottom: 12px;
      display: none;
    }
    .success-container {
      text-align: center;
      padding: 32px 0;
      display: none;
    }
    .success-icon {
      width: 48px;
      height: 48px;
      border-radius: 50%;
      background: rgba(34, 197, 94, 0.1);
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 0 auto 16px;
    }
    .success-icon svg {
      width: 24px;
      height: 24px;
      color: #22c55e;
    }
    .success-container h2 {
      font-size: 18px;
      font-weight: 600;
      margin-bottom: 8px;
    }
    .success-container p {
      font-size: 13px;
      color: #a1a1aa;
      line-height: 1.6;
    }
    .hint {
      text-align: center;
      margin-top: 16px;
      font-size: 11px;
      color: #52525b;
    }
    .hint a {
      color: #2563eb;
      text-decoration: none;
    }
    .hint a:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <div class="container">
    <div class="logo">InputLayer</div>
    <p class="subtitle">Get access to the interactive demo</p>

    <div class="card">
      <div id="form-container">
        <div class="card-header">
          <svg class="card-header-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <rect width="20" height="16" x="2" y="4" rx="2"/>
            <path d="m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7"/>
          </svg>
          <div class="card-header-text">
            <h2>Request Access</h2>
            <p>Enter your email to receive demo credentials</p>
          </div>
        </div>

        <div class="error" id="error"></div>

        <form id="access-form" onsubmit="return handleSubmit(event)">
          <div class="field">
            <label for="email">Email address</label>
            <input type="email" id="email" name="email" placeholder="you@company.com" required autofocus>
          </div>
          <input type="hidden" id="kg" name="kg" value="${defaultKg}">
          <button type="submit" class="btn" id="submit-btn">Get Demo Access</button>
        </form>
      </div>

      <div class="success-container" id="success-container">
        <div class="success-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M20 6 9 17l-5-5"/>
          </svg>
        </div>
        <h2>Check your email</h2>
        <p>
          We've sent a link to <strong id="success-email"></strong> with access to the demo.
          Click the link to open the Studio with everything pre-configured.
        </p>
      </div>
    </div>

    <p class="hint">
      Already have credentials? <a href="/query">Go to Studio</a>
    </p>
  </div>

  <script>
    // Read kg from URL params if present
    (function() {
      var params = new URLSearchParams(window.location.search);
      var kg = params.get('kg');
      if (kg) {
        document.getElementById('kg').value = kg;
      }
    })();

    function handleSubmit(e) {
      e.preventDefault();
      var email = document.getElementById('email').value.trim();
      var kg = document.getElementById('kg').value;
      var btn = document.getElementById('submit-btn');
      var errorEl = document.getElementById('error');

      if (!email) return false;

      btn.disabled = true;
      btn.textContent = 'Sending...';
      errorEl.style.display = 'none';

      fetch('/demo/api/request-access', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email, kg: kg }),
      })
        .then(function(res) { return res.json(); })
        .then(function(data) {
          if (data.error) {
            errorEl.textContent = data.error;
            errorEl.style.display = 'block';
            btn.disabled = false;
            btn.textContent = 'Get Demo Access';
            return;
          }
          // Show success
          document.getElementById('form-container').style.display = 'none';
          document.getElementById('success-email').textContent = email;
          document.getElementById('success-container').style.display = 'block';
        })
        .catch(function() {
          errorEl.textContent = 'Something went wrong. Please try again.';
          errorEl.style.display = 'block';
          btn.disabled = false;
          btn.textContent = 'Get Demo Access';
        });

      return false;
    }
  </script>
</body>
</html>`
}
