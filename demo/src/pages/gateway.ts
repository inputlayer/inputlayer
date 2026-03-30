/**
 * Gateway page served at GET /
 *
 * Client-side logic:
 * 1. If ?invite=<hash> is present, claim the invite and store credentials
 * 2. If credentials exist in web storage, redirect to /query
 * 3. Otherwise, redirect to /demo/request-access
 */
export function gatewayPage(): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>InputLayer Demo</title>
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
      text-align: center;
      padding: 24px;
    }
    .spinner {
      width: 32px;
      height: 32px;
      border: 3px solid #27272a;
      border-top-color: #2563eb;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
      margin: 0 auto 16px;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .message { font-size: 14px; color: #a1a1aa; }
    .error { color: #ef4444; margin-top: 12px; font-size: 13px; }
    .link { color: #2563eb; text-decoration: none; }
    .link:hover { text-decoration: underline; }
  </style>
</head>
<body>
  <div class="container">
    <div class="spinner" id="spinner"></div>
    <p class="message" id="message">Loading...</p>
    <p class="error" id="error" style="display:none"></p>
  </div>

  <script>
    (function() {
      var params = new URLSearchParams(window.location.search);
      var inviteHash = params.get('invite');

      // Case 1: Invite link - claim it
      if (inviteHash) {
        claimInvite(inviteHash);
        return;
      }

      // Case 2: Already has credentials - go to studio
      var conn = localStorage.getItem('inputlayer_connection');
      var pw = sessionStorage.getItem('inputlayer_session_pw');
      if (conn && pw) {
        window.location.replace('/query');
        return;
      }

      // Case 3: No credentials - request access
      window.location.replace('/demo/request-access');
    })();

    function claimInvite(hash) {
      document.getElementById('message').textContent = 'Setting up your access...';

      fetch('/demo/api/invites/' + encodeURIComponent(hash) + '/claim', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      })
        .then(function(res) { return res.json(); })
        .then(function(data) {
          if (data.error) {
            showError(data.error);
            return;
          }

          // Compute port to store - must work with the GUI's buildWsUrl logic
          var port = window.location.port
            ? parseInt(window.location.port, 10)
            : (window.location.protocol === 'https:' ? 443 : 80);

          // Store credentials in the same format the GUI expects
          var connection = {
            host: window.location.hostname,
            port: port,
            name: data.kg,
            username: data.username,
          };

          localStorage.setItem('inputlayer_connection', JSON.stringify(connection));
          sessionStorage.setItem('inputlayer_session_pw', data.password);
          localStorage.setItem('inputlayer_selected_kg', data.kg);

          document.getElementById('message').textContent = 'Redirecting to Studio...';
          window.location.replace('/query');
        })
        .catch(function(err) {
          showError('Failed to claim invite. Please try again or request a new one.');
          console.error(err);
        });
    }

    function showError(msg) {
      document.getElementById('spinner').style.display = 'none';
      document.getElementById('message').textContent = '';
      var errorEl = document.getElementById('error');
      errorEl.style.display = 'block';
      errorEl.innerHTML = msg + '<br><br><a class="link" href="/demo/request-access">Request new access</a>';
    }
  </script>
</body>
</html>`
}
