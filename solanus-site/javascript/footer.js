// =============================================================================
// footer.js — the footer's signup → confirm → survey flow.
//
// The flow (all optional; remove the modals from footer.html to disable it):
//   1. Click the Sign up button       → email-modal (collect name + email)
//   2. Submit that                     → confirm-modal ("answer a few questions?")
//   3. "Sure"                          → questionnaire-modal (the survey)
//      "No thanks" / close             → submit with the survey left blank
//   4. Submit the survey               → POST everything to /api/submit-questionnaire
//
// The survey QUESTIONS are not hard-coded here — global.js builds them inside
// #fanForm from global.json, so the site owner edits questions without touching
// this file. We just collect whatever named fields are present at submit time.
//
// Like header.js, the footer markup is injected asynchronously, so we POLL for
// it (capped) and initialise exactly once. That single-owner init is what keeps
// the modal handlers from being bound twice.
// =============================================================================
(function () {
  'use strict';

  // Captured from the email step, then sent with the survey. `hasSubmitted`
  // guarantees we POST at most once per visit no matter which exit path is used.
  var storedEmail = '';
  var storedName = '';
  var hasSubmitted = false;

  function showModal(modal) {
    if (!modal) return;
    modal.classList.remove('hidden'); modal.classList.add('show');
    document.body.classList.add('modal-open');
  }
  function hideModal(modal) {
    if (!modal) return;
    modal.classList.remove('show'); modal.classList.add('hidden');
    document.body.classList.remove('modal-open');
  }

  // Collect every named field in the (data-driven) survey form. Because the
  // questions come from global.json, we read whatever fields exist rather than a
  // fixed list. Blank answers become 'None' so every column has a value.
  function buildPayload() {
    var payload = { rawEmail: storedEmail, firstName: storedName };
    var form = document.getElementById('fanForm');
    if (form) {
      form.querySelectorAll('input[name], select[name], textarea[name]').forEach(function (el) {
        if (el.name === 'email' || el.name === 'firstName') return;
        var v = el.value;
        payload[el.name] = (v == null || String(v).trim() === '') ? 'None' : v;
      });
    }
    return payload;
  }

  async function submitPayload(payload) {
    if (hasSubmitted) return;     // never post twice
    hasSubmitted = true;
    try {
      await fetch('/api/submit-questionnaire', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
    } catch (err) {
      console.error('Signup submission error:', err);
      alert('Sorry, something went wrong. Please try again later.');
    }
  }

  // Closing/finishing the survey submits whatever is filled in (or nothing).
  function finishSurvey() {
    hideModal(document.getElementById('questionnaire-modal'));
    if (hasSubmitted || !storedEmail) return; // no email captured → nothing to send
    submitPayload(buildPayload());
  }

  // Exiting the confirm step (No thanks / close / backdrop) sends the signup
  // with the survey left blank — but only if we actually captured an email.
  function submitWithoutSurvey() {
    if (!hasSubmitted && storedEmail) submitPayload(buildPayload());
  }

  function initFooter() {
    var signupBtns = [document.getElementById('signup-btn'), document.getElementById('signup-btn-2')].filter(Boolean);
    var emailModal = document.getElementById('email-modal');
    var confirmModal = document.getElementById('confirm-modal');
    var questionnaireModal = document.getElementById('questionnaire-modal');
    if (!emailModal || !confirmModal || !questionnaireModal || !signupBtns.length) return;

    var emailForm = emailModal.querySelector('#emailForm');

    // Step 1 → open the email modal.
    signupBtns.forEach(function (btn) {
      btn.addEventListener('click', function (e) { e.preventDefault(); showModal(emailModal); });
    });

    // Step 1 submit → capture name/email, go to the confirm modal.
    if (emailForm) emailForm.addEventListener('submit', function (e) {
      e.preventDefault();
      storedEmail = (emailForm.querySelector('input[name="email"]') || {}).value || '';
      storedName = (emailForm.querySelector('input[name="firstName"]') || {}).value || '';
      hideModal(emailModal); showModal(confirmModal);
    });

    // Step 2 → "Sure" opens the survey; every other exit submits with defaults.
    var yes = document.getElementById('confirm-yes');
    var no = document.getElementById('confirm-no');
    if (yes) yes.addEventListener('click', function () { hideModal(confirmModal); showModal(questionnaireModal); });
    if (no) no.addEventListener('click', function () { hideModal(confirmModal); submitWithoutSurvey(); });

    // Generic close/backdrop handling for each modal.
    emailModal.addEventListener('click', function (e) { if (e.target === emailModal) hideModal(emailModal); });
    confirmModal.addEventListener('click', function (e) { if (e.target === confirmModal) { hideModal(confirmModal); submitWithoutSurvey(); } });
    questionnaireModal.addEventListener('click', function (e) { if (e.target === questionnaireModal) finishSurvey(); });

    var emailClose = emailModal.querySelector('.modal-close');
    if (emailClose) emailClose.addEventListener('click', function () { hideModal(emailModal); });
    var confirmClose = confirmModal.querySelector('.modal-close');
    if (confirmClose) confirmClose.addEventListener('click', function () { hideModal(confirmModal); submitWithoutSurvey(); });
    var qClose = questionnaireModal.querySelector('.modal-close');
    if (qClose) qClose.addEventListener('click', finishSurvey);

    // Step 3 submit.
    var submitBtn = document.getElementById('questionnaire-submit-btn');
    if (submitBtn) submitBtn.addEventListener('click', finishSurvey);
  }

  // Poll (capped) for the injected footer, then wire it up once.
  function waitForFooter() {
    var tries = 0;
    var timer = setInterval(function () {
      if (document.getElementById('email-modal') || document.querySelector('#footer-placeholder footer')) {
        clearInterval(timer); initFooter();
      } else if (++tries > 80) { clearInterval(timer); }
    }, 50);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', waitForFooter);
  else waitForFooter();
})();
