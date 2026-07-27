# Pagesmith — Using the Visual Editor (User Guide)

**Author:** David Falk

**Audience:** Site owners and non-developers who will build and edit the site.

---

## Before we start: what kind of thing is this?

Most website tools you have met work like this: there is the *website*, which the
world sees, and then there is a separate *control room* — a dashboard at some
`/admin` URL, full of forms and tables and settings, that looks nothing like the
finished site. You edit in the control room, click save, and hope the result looks
right on the real page. Then you go look at the real page. Then you go back to the
control room. You spend your day flipping between two places that never quite agree
with each other.

Pagesmith does not work that way, and that single difference is the whole spirit of
this guide. **The Pagesmith editor turns your own live page into the thing you
edit.** There is no separate control room. You are looking at the real site —
the real headline, the real photos, the real layout — and you click directly on
the words to change the words, and directly on a photo to change the photo. What
you see is, very literally, what you get, because the thing you are editing *is*
the thing visitors will see.

This makes the editor friendly, but it also creates one small mental adjustment we
will keep coming back to: because you are standing on the live page, the editor has
to be careful about *when* a change becomes real. Some changes happen the instant
you make them; a few important ones are deliberately "staged" and wait for you to
confirm. We will flag every one of those moments. None of them can hurt you — the
editor is built so that the worst thing an accidental click can do is make you press
"Undo".

> **A reassuring promise up front.** Nothing you do in the editor reaches the public
> until you press one specific button (**Publish**). Until then you are quietly
> working on a private *draft* that only you can see. You can experiment, make a
> mess, change your mind, and reload the page to throw away anything you didn't save.
> Read that sentence again if you are nervous. It is the safety net under everything
> that follows.

---

## Table of contents

1. [What the editor *is*](#1-what-the-editor-is)
2. [Running it on your own computer](#2-running-it-on-your-own-computer)
3. [A tour of the toolbar](#3-a-tour-of-the-toolbar)
4. [Editing the content that's already there](#4-editing-the-content-thats-already-there)
5. [The Sections panel — and the one big gotcha](#5-the-sections-panel--and-the-one-big-gotcha)
6. [Adding a new section](#6-adding-a-new-section)
7. [Reordering things](#7-reordering-things)
8. [Theme & Background](#8-theme--background)
9. [Pages and the three visibility levels](#9-pages-and-the-three-visibility-levels)
10. [The signup survey: Questionnaire & Responses](#10-the-signup-survey-questionnaire--responses)
11. [Tracking (analytics & pixels)](#11-tracking-analytics--pixels)
12. [Saving & publishing — the mental model](#12-saving--publishing--the-mental-model)
13. [The "?" help buttons](#13-the--help-buttons)
14. [Your first edit (a 60-second tutorial)](#14-your-first-edit-a-60-second-tutorial)

---

## 1. What the editor *is*

The editor is a thin layer of in-browser software that **only appears under two
conditions at once**. Understanding those two conditions explains a lot of
otherwise-mysterious behaviour, so let's be precise.

**Condition one: you must be on a *draft* host.** Every Pagesmith site has at least
two "addresses" it can live at. One is the real, public address your visitors use
(the *live* or *production* host). The other is a private *draft* host — usually
`localhost` on your own machine while you work, or a quiet preview URL nobody knows
about. The editor flatly refuses to load on the live host. This is on purpose: it
means a visitor can never stumble onto editing controls, and you can never
accidentally start editing the public site directly. You edit the draft; the live
site only changes when you Publish.

**Condition two: you must be signed in as an *editor*.** When you open the site, it
checks who you are. If you are just a visitor, you see the site. If you are signed in
and your account carries the **`editor`** role, a slim toolbar fades in along the top
of the page. That toolbar is the entire editor. There is nothing else to find.

```
   ┌──────────────────────────────────────────────────────────────────────┐
   │  Is this the LIVE (public) host?  ──── yes ───►  no editor, ever.      │
   │            │                                                           │
   │            no (it's a draft host)                                      │
   │            │                                                           │
   │            ▼                                                           │
   │  Am I signed in WITH the "editor" role?  ── no ──►  you see the site,  │
   │            │                                        but no toolbar.    │
   │            yes                                                         │
   │            ▼                                                           │
   │     ✎  The Edit toolbar appears.  You may now edit the draft.          │
   └──────────────────────────────────────────────────────────────────────┘
```

Why build it this way? Because it keeps the public site *pure*. The code that
renders your live page is exactly the code that renders it for visitors — the editor
is bolted on top only in the draft world, and it leaves no trace on the real one.
You never have to wonder "is this an editor thing or a real thing?" On the live site
there are no editor things.

---

## 2. Running it on your own computer

You can run the entire site — backend and all — on your own laptop, with no cloud
account and no money spent. This is the safest possible playground: it is a private
draft host by definition, so the editor is available, and nothing you do can reach
the internet.

### Prerequisites (install these once)

You need **Node.js version 20**, and three command-line tools installed globally.
On a terminal, that is:

```bash
npm i -g azurite @azure/static-web-apps-cli azure-functions-core-tools@4
```

What those three are, in plain terms:

- **azurite** — a tiny local stand-in for cloud storage. It pretends to be the place
  your content and images live, so you don't need a real cloud account to try things.
- **@azure/static-web-apps-cli** (the "SWA emulator") — runs the website itself on
  your machine, exactly the way the cloud would.
- **azure-functions-core-tools@4** — runs the small backend that saves your edits.

You install these once and forget about them.

### Start it

From the project folder, run:

```bash
./start-local.sh
```

That one script does everything: it starts the local storage, fills it with the
starter content, and launches the site. When it finishes you'll see a line telling
you where to go. Open a browser to:

```
http://localhost:4280
```

### Become an editor

The first time you open the site, it sends you to a sign-in screen. This is the
local emulator's pretend login — it is not asking for a real password. Here is the
exact dance:

1. Sign in as the user **`dev`**.
2. On that same screen, **add the role `editor`** (there's a field for roles; type
   `editor` into it).
3. Continue.

The page reloads, and now — because you are on a draft host *and* you carry the
`editor` role — the **✎ Edit** toolbar appears across the top. You're in.

> **If the toolbar doesn't show up**, it is almost always the role. Sign out, sign
> back in, and make sure you typed `editor` (lowercase) into the roles box. Without
> that role you are treated as an ordinary visitor, which is the editor working
> exactly as designed.

```
  ./start-local.sh ──► local storage + site come up ──► open localhost:4280
        │
        ▼
  sign-in screen ──► user: dev, role: editor ──► ✎ Edit toolbar appears
```

---

## 3. A tour of the toolbar

Everything you can do lives in that one bar at the top. Here is the whole thing,
left to right:

```
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │ ● wordmark editor │ [Edit][Preview] │ « Home »│ Sections │ Pages │ Save draft │ Publish │ More ▾ │
 └──────────────────────────────────────────────────────────────────────────────────────┘
        (1)               (2)            (3)        (4)       (5)      (6)        (7)      (8)

   More ▾  opens:
   ┌───────────────────────────────────────────────────────────────────┐
   │ Theme · Background · Questionnaire · Tracking · Responses ·         │
   │ Refresh stats · Revert · Help · Log out                            │
   └───────────────────────────────────────────────────────────────────┘
```

Let's walk through each control. One short paragraph each — you don't have to
memorise these; just know they exist and roughly what they're for.

**(1) The brand corner.** A little recording-style dot, your site's wordmark, and the
word "editor". The dot is a gentle reminder that you are editing a *private draft* —
think of it as a "rec" light telling you the changes here are not live yet. It's
decoration, not a button.

**(2) Edit / Preview.** The master switch. **Edit** turns on the editing layer: a soft
dashed outline appears around everything you can change, so you can see your
canvas. **Preview** hides every editing control so you see the page exactly as a
visitor would — no outlines, no handles, nothing. Flip to Preview any time you want
to check your work; flip back to Edit to keep going. Nothing is saved or discarded by
toggling this; it's purely "show me the controls / hide the controls".

**(3) The page pill.** Shows which page you're currently editing (e.g. "Home"). It also
quietly tells you the page's state: it will say something like "Home — draft" normally,
and "Home — unsaved" when you have changes you haven't saved yet. A handy at-a-glance
"do I have unsaved work?" indicator.

**(4) Sections.** Opens the **Sections panel**, where you add, hide, delete, and reorder
the big building blocks of the current page. This is the most important panel and it
has one quirk we'll cover carefully in [section 5](#5-the-sections-panel--and-the-one-big-gotcha).

**(5) Pages.** Opens the **Pages panel**: create new pages, and set each page's
visibility (Public / Unlisted / Dev only). Covered in [section 9](#9-pages-and-the-three-visibility-levels).

**(6) Save draft.** Saves everything you've changed *to your private draft*. Only you
can see a draft. This is the button you press often and without fear. It does **not**
make anything public.

**(7) Publish.** The one button that reaches the world. It copies your saved draft onto
the live, public site. Visitors see the change after this and only after this. There's
a confirmation, and it publishes your last *saved* draft, so the habit is: Save, look,
*then* Publish.

**(8) More ▾.** A dropdown holding the less-frequent tools, so the bar stays tidy:

- **Theme** — site-wide colours.
- **Background** — the big background image behind a page.
- **Questionnaire** — edit the signup-survey questions.
- **Tracking** — paste in analytics / pixel IDs.
- **Responses** — download the survey answers people have submitted (as a file).
- **Refresh stats** — pulls live numbers (where the site is wired to a data source)
  into a stats page draft. Safe to ignore unless your site uses it.
- **Revert** — throw away all your *unpublished* changes and snap back to whatever is
  currently live. Your emergency "undo everything since the last Publish" button.
- **Help** — the built-in help (see [section 13](#13-the--help-buttons)).
- **Log out** — sign out of the editor.

---

## 4. Editing the content that's already there

This is the part that makes Pagesmith feel like magic the first time. With **Edit**
turned on, you change things by clicking the actual thing on the page.

### Text: click it and type

Click on a headline, a paragraph, a button label — any text with that dashed outline —
and a cursor appears right inside it. Type. Backspace. It behaves like a text box,
except the "text box" *is* the headline on your page, sized and styled exactly as it
will look when published. Click away when you're done.

Behind the scenes, each edit is written straight into the page's draft as you type,
and the page pill flips to "— unsaved" to remind you to **Save draft** before you
leave. (Until you Save, your changes live only in this browser tab; reloading the
page throws them away. That's the safety net, not a bug.)

> **A small honesty note about pasting.** Typing plain text is perfect. If you *paste*
> text copied from, say, a Word document or a web page, the browser sometimes smuggles
> in invisible formatting along with it. For the cleanest result, paste as plain text
> (often Ctrl/Cmd-Shift-V) or just type. Nothing breaks if you don't — it's just tidier.

### Images: click to open the media picker

Click an image and the **media picker** opens. This is a little gallery of pictures
that already live in your site, organised into folders (tabs) across the top. You can:

- **Choose an existing image** — click any thumbnail to use it.
- **Upload a new one** — there's an upload control; pick a file from your computer and
  it's added to the library and selected.

Pick one, and the image on the page swaps to it immediately. (For most images the site
optimises uploads automatically so pages stay fast; you don't have to think about it.)

### Per-element text styling

Sometimes you want *this one* heading bigger, or *this one* paragraph centred, without
touching the rest of the site. That's per-element styling, and you reach it through the
section's **Edit** panel (more on sections next). Inside that panel you'll find small
controls for things like alignment (Left / Center / Right), text colour, and size for the
individual pieces of text in that section. Leave a styling field blank and that element
simply inherits the site-wide look — so "empty" here means "use the default", not
"make it blank". This is the gentle rule throughout the editor: blank = inherit.

---

## 5. The Sections panel — and the one big gotcha

A page is built from **sections** stacked top to bottom: a headline section, a photo
gallery, a block of text, a video, and so on. The **Sections** button opens the panel
that manages them.

Each section gets a row with a few controls:

```
   ┌─────────────────────────────────────────────────────────────┐
   │  ⠿ drag   Heading: "Welcome"        [Hide] [View] [Edit] [Delete] │
   │  ⠿ drag   Photo gallery (8)         [Hide] [View] [Edit] [Delete] │
   │  ⠿ drag   Text: "About us"          [Hide] [View] [Edit] [Delete] │
   │                                                               │
   │  + Add section                                                │
   │                                                               │
   │  Across the whole site                                        │
   │     Header / nav                          [View] [Edit]       │
   │     Footer                                 [View] [Edit]       │
   │                                                               │
   │            [ Cancel ]            [ Apply & Save ]             │
   └─────────────────────────────────────────────────────────────┘
```

- **⠿ drag** — grab here to reorder (see [section 7](#7-reordering-things)).
- **Hide** — takes a section off the page but keeps it in the list, so you can **Show**
  it again later. Perfect for something seasonal you'll want back (a sale banner, an
  event poster between events). Nothing is lost.
- **View** — jumps the page to that section so you can see what you're working on.
- **Edit** — opens that section's content (its text, its images, its list of items).
- **Delete** — removes an *added* section. A little **Undo** appears right after, in case
  it was a mistake.

The two site-wide rows at the bottom ("Across the whole site" — your header/nav and
footer) only offer **View** and **Edit**, because they appear on every page; you
wouldn't hide or delete them from a single page.

### Now, the gotcha. Read this slowly.

**The Sections panel is a *staging* editor.** When you Hide, Delete, reorder, or Add in
this panel, *the live page underneath does not change as you click.* Your clicks are
collected, like writing a shopping list, and nothing happens to the actual page until
you press **Apply & Save**. Press **Cancel** and the whole list is forgotten — the page
is untouched.

This trips people up because the rest of the editor is so immediate (click text, text
changes). Here, on purpose, it is not. Why? Because reordering and deleting whole
sections are big, structural moves, and it's much safer to plan them all out and apply
them in one deliberate step than to have the page leaping around under your cursor.

```
   You click Hide / Delete / reorder  ──►  staged in the panel (page unchanged)
                                              │
                                       press Apply & Save
                                              │
                                              ▼
                                   the page actually changes  (draft saved)
```

So if you Delete a section and glance at the page and it's *still there* — that's
correct. It will go when you Apply & Save. And **Delete has an Undo right up until you
save**: undo it in the panel and it's as if you never touched it. Once you Apply &
Save, the deletion is committed to your draft (though even then, remember, nothing is
*public* until you Publish, and **Revert** can still rescue you).

> **The two-word rule of thumb:** *Hide* means "not now, maybe later" — reversible any
> time, forever. *Delete* means "gone" — but only undoable until you save. When in
> doubt, Hide.

---

## 6. Adding a new section

Inside the Sections panel, click **+ Add section**. This opens the **parts library** —
a grid of tiles, one per kind of section you can add: Text, Heading, Photo gallery,
Video, Carousel, Links, and more, each with a little preview.

```
   ┌──────── + Add section: pick a part ────────┐
   │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐    │
   │  │ Text │  │Headng│  │Gallery│ │ Video│    │
   │  └──────┘  └──────┘  └──────┘  └──────┘    │
   │  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐    │
   │  │Carsel│  │ Links│  │ Quote│  │ ...  │    │
   │  └──────┘  └──────┘  └──────┘  └──────┘    │
   └────────────────────────────────────────────┘
```

Click a tile and that part **drops into your page pre-filled** with placeholder
content — a sample heading, a couple of example items, whatever makes sense for that
part. You never face a scary blank box. From there you edit it like anything else:
click the placeholder text to replace it, click an image to swap it.

> **The tiles are clickable cards, not buttons** — so just click anywhere on the tile.
> (We mention this only because it occasionally surprises people who go hunting for a
> button to press.)

### Lists inside a section (galleries, links, etc.)

Some sections aren't a single thing — they're a *list* of things: a gallery is a list
of images, a "links" block is a list of links, a carousel is a list of slides. You
**edit those lists from the section's Edit panel**, not by hovering on the page. Open
the section's **Edit**, and you'll see a button like **"Edit gallery (8)"** or **"Edit
links (3)"** — the number tells you how many items are in there. Click it to open the
list editor, where you can add items, fill in each one's fields (text, a URL, an
image, and so on), and reorder them.

This is a deliberate design choice — there's a single, predictable place to manage a
list (the section's Edit panel) rather than a scatter of hover controls. So if you ever
think "where did the images go, I can't find them to edit" — they're behind the
section's **Edit → Edit gallery (N)**.

---

## 7. Reordering things

Two ways, depending on what you're moving.

**Sections on a page:** in the Sections panel, grab a row by its **⠿ drag** handle and
drag it up or down to a new position. (Remember: this is staged — press **Apply &
Save** to make it real.) You can also drag a list *item* directly on the page to
reorder it: hover over an item and a small grip appears; drag it.

**Items inside a Group:** some parts are "Groups" — a section that holds a few
sub-items side by side. Inside a Group you'll find small **Up / Down** controls on each
item to nudge it one step at a time, which is often easier than dragging when things
are close together.

```
   Drag handle:   ⠿  ◄── grab here, drag the whole row up/down

   Inside a Group:   [item]  [▲ Up] [▼ Down]
```

---

## 8. Theme & Background

These two live in the **More ▾** menu and control how the site *looks* rather than what
it *says*.

### Theme — site-wide colours

**Theme** opens a colour panel. Change the main accent colour (the colour of buttons,
links, highlights) and a couple of others, and the whole site shifts to match. There's
also a palette used by chart-like and decorative parts. Theme changes are **site-wide** —
they affect every page.

If you want to restyle *just one section* instead of the whole site, don't use Theme —
use that section's own styling (via its **Edit** panel, [section 4](#4-editing-the-content-thats-already-there)).
The rule: Theme = everything; section styling = this one section.

> **A small known wrinkle:** if you *clear* one of the three primary theme colours
> hoping it falls back to the built-in default, the live page may keep showing the old
> colour until you reload the page. Reloading sorts it out. (Setting a new colour
> always works immediately; it's only the "clear back to default" case that needs a
> refresh.)

### Background — the big image behind a page

**Background** sets the large image that sits behind the page content. It opens the same
media picker you already know — choose an existing image or upload a new one. You can
set a different image for **large screens (desktop/tablet)** and for **phones**, so the
background looks right on both.

> If you upload a new background but **reuse a filename you've used before**, your
> browser may show the old cached version. A hard refresh (Ctrl/Cmd-Shift-R) forces it
> to fetch the new one. The editor will even hint this to you when it happens.

---

## 9. Pages and the three visibility levels

The **Pages** button opens the Pages panel, where you create pages and decide who can
reach them.

**Creating a page:** type a name (e.g. "Press") and create it. The new page starts
empty — open it and build it up from the parts library exactly like any other page.
There's no code to write and no setup; a fresh page just works at its own URL the
moment you save it.

**Visibility** is the important concept here. Every page has one of three levels, and
each means something specific for your *live* site:

```
   ┌────────────┬───────────────────────────────┬──────────────────────────────┐
   │ Level      │ In the live menu?             │ Reachable by direct link?     │
   ├────────────┼───────────────────────────────┼──────────────────────────────┤
   │ Public     │ Yes — shown to everyone       │ Yes                          │
   │ Unlisted   │ No — hidden from the menu      │ Yes — if you share the link  │
   │ Dev only   │ No                             │ No — never published at all  │
   └────────────┴───────────────────────────────┴──────────────────────────────┘
```

- **Public** — the normal case. The page appears in your site's navigation menu and
  anyone can find it. Most pages are Public.
- **Unlisted** — the page is *not* in the menu, but anyone you give the link to can open
  it. Ideal for a press kit, a private preview, or a page you only want to share
  directly rather than advertise.
- **Dev only** — the page stays on your private draft and is **never published**. Use it
  for works-in-progress and admin-only pages. When you Publish, Dev-only pages are
  skipped automatically — they simply don't exist on the public site.

New pages start as Public; set them to Unlisted or Dev only if they aren't ready for
prime time. You can change a page's visibility any time, and the change is saved as
soon as you pick it.

---

## 10. The signup survey: Questionnaire & Responses

Pagesmith includes a built-in signup flow — a way for visitors to give you their email
and answer a few questions. There are two tools for it in the **More ▾** menu:
**Questionnaire** (edit the questions) and **Responses** (download the answers).

The visitor's experience flows like this:

```
   ┌────────────┐      ┌─────────────┐      ┌──────────────┐
   │  Sign up   │ ───► │   Confirm   │ ───► │  Survey       │
   │ (email in) │      │ (thank you) │      │ (your Qs)     │
   └────────────┘      └─────────────┘      └──────────────┘
        visitor enters email   →   sees confirmation   →   answers your questions
```

### Editing the questions (Questionnaire)

Open **Questionnaire** and you can edit the intro text and the list of questions. Use
**Edit questions** to add, remove, and reorder them. A few honest details worth knowing,
because the editor is candid about edge cases:

- **A question with a blank label is skipped entirely.** So "an empty question box" means
  "no question here", not "a blank question". To remove a question, you can clear it.
- The **email box is always present** — that's the core of a signup; you don't have to add
  it.
- **Renaming a question keeps its answers lined up.** Each question becomes a *column* in
  the downloaded responses, and renaming a question doesn't scramble the answers people
  already gave. Removing a question doesn't touch answers already submitted, either.

### Where responses go (Responses)

Click **Responses** and the answers people have submitted download to your computer as a
file (CSV — the kind a spreadsheet opens; a JSON option is available too). Each question
is a column, each submission a row. That's the whole loop: you set the questions here,
visitors answer them on the live site, and you collect the results with one click.

---

## 11. Tracking (analytics & pixels)

If you want to measure traffic — with Google Analytics, a Meta (Facebook) Pixel, or
similar — open **Tracking** from the **More ▾** menu and paste in your IDs. There's a
field for each; paste, save, done. This is entirely optional; the site works perfectly
without it.

> **One important rule:** tracking only fires on the **live** site. It deliberately does
> *not* run on your draft host. That's a feature — it means your own editing and testing
> never pollutes your real analytics with fake visits. So don't be alarmed if you paste a
> tracking ID and see no activity while you're editing locally; it switches on once the
> page is published and a real visitor loads it.

---

## 12. Saving & publishing — the mental model

This is the most important section to internalise, and it's genuinely simple once it
clicks. There are two copies of your site: a private **draft** and the public **live**
site. Three buttons move things between them.

```
        ┌──────────────────┐                         ┌──────────────────┐
        │      DRAFT        │   ── Publish ──►        │       LIVE        │
        │  (private, only   │                         │   (public, what   │
        │   you can see)    │   ◄── Revert ──         │  visitors see)    │
        └──────────────────┘                         └──────────────────┘
              ▲      │
              │      │  reload the page = throw away anything unsaved
   Save draft │      │
              │      ▼
        your in-progress edits in this browser tab
```

- **Save draft** = write your current edits into the private draft. Now they're safely
  stored (they'll survive a reload), but still private. Press this often.
- **Publish** = copy the saved draft onto the live, public site. *This is the only step
  that visitors ever see.* Publish takes your last **saved** draft — so the reliable habit
  is **Save → look it over → Publish**.
- **Revert** = discard every unpublished change and restore whatever is currently live.
  Your big "undo everything since the last Publish" rescue button. (Dev-only pages are
  never published, so Publish skips them automatically.)

> **One thing that surprises people: some panels reload the page after you save.** When
> you finish in certain panels (a list editor, the Theme panel, adding a section), the
> page refreshes itself to redraw cleanly. This is completely normal and expected — it's
> not an error and you didn't lose anything; your save already happened. Just let it
> settle.

And the safety net, one more time, because it's worth repeating: until you press
**Publish**, the public never sees a thing. Until you press **Save draft**, even your
draft doesn't have it — a reload returns you to the last saved state. Between those two
facts, it's very hard to do real damage.

---

## 13. The "?" help buttons

You don't have to keep this guide open while you work. **Next to the heading of almost
every panel there's a small round "?" button.** Click it and a short, plain-language
help note opens, written for exactly the panel you're looking at — what the buttons do,
what happens if you leave a field blank, the little edge cases. There's also a general
**Help** entry in the **More ▾** menu for the overview.

These notes are deliberately honest about the fiddly bits (the same edge cases this
guide calls out — blank questions being skipped, "empty means inherit", the cache-refresh
quirk). So when you're mid-task and wondering "wait, what does *this* do?", the answer is
one click away, right where you are.

---

## 14. Your first edit (a 60-second tutorial)

Let's prove the whole loop works, end to end, with the smallest possible change:
editing the home page headline and watching it persist.

```
   1. Turn on editing        →  click  [Edit]  in the toolbar.
                                 verify: a dashed outline appears around editable things.

   2. Find the headline      →  scroll to the big headline on the Home page.
                                 verify: hovering it shows the dashed outline.

   3. Change it              →  click the headline, select the text, type something new.
                                 verify: the headline on the page now shows your new words,
                                         and the page pill reads "Home — unsaved".

   4. Save it                →  click  [Save draft].
                                 verify: the pill changes back to "Home — draft"
                                         (no longer "unsaved").

   5. Prove it persisted     →  reload the browser (F5 / Cmd-R).
                                 verify: your new headline is STILL there.
                                         (If you had skipped step 4, the reload would
                                          have brought back the old headline — that's the
                                          safety net doing its job.)
```

That's the entire rhythm of working in Pagesmith, in miniature: **Edit → change →
Save → it sticks.** Everything else in this guide is just more *kinds* of change
(images, sections, pages, colours) hung on that same simple loop. When you're ready
to share your work with the world, you do it one more way — **Publish** — and your saved
draft becomes the live site.

Welcome aboard. Go make something.
