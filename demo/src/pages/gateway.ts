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
      // Restore password from localStorage into sessionStorage if needed
      // (sessionStorage is cleared on tab close, but we persist in localStorage for demo convenience)
      var conn = localStorage.getItem('inputlayer_connection');
      var pw = sessionStorage.getItem('inputlayer_session_pw');
      if (!pw) {
        pw = localStorage.getItem('inputlayer_session_pw');
        if (pw) sessionStorage.setItem('inputlayer_session_pw', pw);
      }
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
          // Store in both sessionStorage (GUI reads this) and localStorage (survives tab close)
          sessionStorage.setItem('inputlayer_session_pw', data.password);
          localStorage.setItem('inputlayer_session_pw', data.password);
          localStorage.setItem('inputlayer_selected_kg', data.kg);
          // Clear stale history from previous sessions
          localStorage.removeItem('inputlayer_query_history');
          // Set welcome editor content with example queries for the KG
          localStorage.setItem('inputlayer_editor_content', getWelcomeContent(data.kg));

          document.getElementById('message').textContent = 'Redirecting to Studio...';
          window.location.replace('/query');
        })
        .catch(function(err) {
          showError('Failed to claim invite. Please try again or request a new one.');
          console.error(err);
        });
    }

    function getWelcomeContent(kg) {
      var content = {
        'default': [
          '// Welcome to InputLayer!',
          '//',
          '// This knowledge graph models a small e-commerce store.',
          '// There are customers with different tiers (premium, standard),',
          '// products with inventory levels, and orders linking them together.',
          '// InputLayer automatically derives which customers are active,',
          '// which orders can be fulfilled, and computes revenue — all from',
          '// simple facts and rules. No imperative code needed.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '// Then uncomment the others one at a time to explore.',
          '',
          '// Start simple: who are the active customers?',
          '?active_customer(Id, Name)',
          '',
          '// What products are currently in stock?',
          '// ?in_stock(Pid, Name)',
          '',
          '// Alice is a premium customer. What can she actually buy?',
          '// InputLayer chains rules: active customer + in-stock product + tier check',
          '// ?can_purchase("c1", Pid, Name, Price)',
          '',
          '// Which orders have everything they need to ship?',
          '// ?fulfillable_order(Oid, Cname, Pname, Qty)',
          '',
          '// And the bottom line — total revenue across all fulfilled orders',
          '// ?total_revenue(R)',
        ],
        'flights': [
          '// Welcome to the Flight Reachability demo!',
          '//',
          '// Imagine a network of airline routes connecting cities worldwide.',
          '// You know the direct flights, but the interesting question is:',
          '// "where can I actually get to, with any number of connections?"',
          '// InputLayer answers this with recursive rules — it follows the',
          '// route graph to its logical conclusion, automatically.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '',
          '// From New York, where in the world can you fly?',
          '?can_reach("new_york", Dest)',
          '',
          '// Which city has the most reachable destinations?',
          '// ?reachable_count(City, N)',
          '',
          '// Starting from Berlin, which continents can you reach?',
          '// ?reachable_continent("berlin", Continent)',
          '',
          '// Show me all one-stop connections from New York',
          '// ?one_stop("new_york", Dest, Via)',
          '',
          '// Now the fun part: add a brand new route and watch',
          '// all the derived conclusions update instantly.',
          '// Uncomment both lines below and run them together:',
          '// +direct_flight("sydney", "cape_town", "southern_jet", 14.0)',
          '// ?can_reach("new_york", "cape_town")',
        ],
        'rules_vectors': [
          '// Welcome to the Rules + Vector Search demo!',
          '//',
          '// This is where traditional reasoning meets AI-style similarity.',
          '// A customer wants to buy printer ink. First, InputLayer uses rules',
          '// to figure out if the customer is eligible (good standing, no disputes).',
          '// Then it uses vector similarity to rank compatible products by how',
          '// closely they match what the customer is looking for.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '',
          '// First, which customers are in good standing?',
          '?good_standing(C)',
          '',
          '// What ink cartridges can customer 1 actually purchase?',
          '// (This chains: good standing -> owns printer -> compatible ink)',
          '// ?can_purchase("cust_1", Pid, Name)',
          '',
          '// Now rank products by vector similarity to a target embedding.',
          '// This combines rule-based eligibility with cosine distance:',
          '// ?eligible("cust_1", Pid), product(Pid, Name, Emb), Dist = cosine(Emb, [0.80, 0.43, 0.90]), Dist < 0.2',
        ],
        'retraction': [
          '// Welcome to the Correct Retraction demo!',
          '//',
          '// This tackles a subtle but critical problem: what happens when',
          '// multiple reasons lead to the same conclusion, and you remove one?',
          '// Customer 42 is blocked for two reasons: unpaid bills AND an',
          '// unverified payment card. If we resolve the unpaid bill, are they',
          '// still blocked? (Yes — the card issue remains.) Most systems get',
          '// this wrong. InputLayer handles it correctly.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '',
          '// Is the customer currently blocked?',
          '?blocked(C)',
          '',
          '// Why are they blocked? How many reasons?',
          '// ?block_count(C, N)',
          '',
          '// Now remove one blocking reason and check again.',
          '// The customer should STILL be blocked (unverified card remains):',
          '// -unpaid_bill("customer_42", "inv_301", 450.00)',
          '// ?blocked("customer_42")',
          '',
          '// What can they purchase once unblocked?',
          '// ?can_purchase(C, Name)',
        ],
        'incremental': [
          '// Welcome to the Incremental Updates demo!',
          '//',
          '// This shows an org chart where authority flows downward.',
          '// Alice is CEO, and manages Bob and Carol, who each manage',
          '// their own teams. InputLayer recursively computes who has',
          '// authority over whom. When a new employee joins or a reporting',
          '// line changes, only the affected conclusions are recomputed —',
          '// not the entire graph.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '',
          '// Who does Alice (the CEO) have authority over?',
          '?authority("alice", Who)',
          '',
          '// How many people report (directly or indirectly) to each manager?',
          '// ?authority_count(Mgr, N)',
          '',
          '// How many levels deep is each employee from the CEO?',
          '// ?chain_length(E, L)',
          '',
          '// Add a new hire under Frank and watch the authority chain update:',
          '// +employee("kate", "Kate Wilson", "Engineer")',
          '// +manages("frank", "kate")',
          '// ?authority("alice", "kate")',
        ],
        'provenance': [
          '// Welcome to the Provenance demo!',
          '//',
          '// An AI agent is approving purchase orders. But you need to audit',
          '// its decisions: why was this order approved? Why was that one denied?',
          '// InputLayer tracks the full derivation chain — every conclusion',
          '// traces back to the specific facts and rules that produced it.',
          '// Use .why and .why_not to inspect the reasoning.',
          '//',
          '// Try running the query below (Cmd+Enter or click the Run button).',
          '',
          '// Which purchase orders got approved?',
          '?purchase_ok(T, V, Amt, Item)',
          '',
          '// Flag high-risk purchases (vendor risk score > 0.2)',
          '// ?high_risk_purchase(T, V, Amt, Risk)',
          '',
          '// Which orders were denied, and why?',
          '// ?order_denied(T, V)',
          '',
          '// Budget utilization: how much has each team spent?',
          '// ?budget_utilization(T, Total, Budget, Pct)',
          '',
          '// The killer feature: ask InputLayer WHY a decision was made.',
          '// This shows the full proof tree:',
          '// .why ?purchase_ok("team_alpha", "acme_supplies", 3200, "Server rack")',
        ],
      };
      return (content[kg] || content['default']).join('\\n');
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
