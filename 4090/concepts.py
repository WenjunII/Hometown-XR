"""
Semantic concept anchors for the second-stage similarity scoring.

These sentences describe the *concepts* we're looking for. The multilingual
sentence-transformer model maps them to a shared embedding space, so an
English anchor sentence will match semantically similar content written
in any of 50+ supported languages.

Anchors are written in a PERSONAL NARRATIVE voice — they read like someone
telling their own story. This biases the semantic model toward matching
actual personal narratives rather than encyclopedic or commercial text.

Anchors are encoded once at startup and compared against candidate paragraphs.
"""

CONCEPT_ANCHORS = [
    # ── Hometown & Place of Origin ───────────────────────────────────────
    "I was born and raised in a small town. Every time I go back, "
    "I recognize the streets and houses from my childhood.",

    "I grew up in a village surrounded by fields and forests. "
    "The landscape of my hometown is etched into my memory.",

    "When I returned to the town where I spent my childhood, "
    "I felt overwhelming emotion and a deep sense of connection.",

    # ── Childhood & Growing Up ───────────────────────────────────────────
    "My earliest memories are of playing outside near our family home. "
    "Those carefree days shaped who I became.",

    "Growing up in my parents' house, I learned the values and "
    "traditions that would stay with me for the rest of my life.",

    "I remember my childhood vividly — the sounds, the smells, "
    "the rhythm of daily life in the neighborhood where I was raised.",

    # ── Belonging & Community ────────────────────────────────────────────
    "I finally found a community where I truly belong. "
    "For the first time in my life, I feel accepted and at home.",

    "Home for me is not just a building — it is the feeling of being "
    "among my own people, where I am understood and loved.",

    "After years of searching, I realized that belonging is not about "
    "a place but about the people who make me feel like myself.",

    # ── Roots & Heritage ─────────────────────────────────────────────────
    "When I visit the village where my grandparents grew up, "
    "I feel a deep connection to my family's history and traditions.",

    "My grandmother used to tell me stories about our ancestors. "
    "Those stories made me proud of where my family comes from.",

    "I decided to trace my family's roots back to the old country. "
    "Discovering my heritage gave me a new sense of identity.",

    # ── Nostalgia & Homecoming ───────────────────────────────────────────
    "After living abroad for many years, I ache with longing for "
    "my homeland and the simple life I once knew there.",

    "I miss my hometown terribly — the familiar faces, the food, "
    "the sound of my mother tongue spoken on every corner.",

    "When I finally came back to the place where I grew up after "
    "so many years away, tears streamed down my face.",

    # ── Diaspora & Displacement ──────────────────────────────────────────
    "As an immigrant, I carry two worlds inside me. My heart is "
    "split between the country I left and the one I now call home.",

    "Being part of the diaspora means I am caught between cultures, "
    "always longing for a home that may no longer exist as I remember.",

    "My family was forced to leave our homeland, and starting over "
    "in a new country was the hardest thing I have ever done.",

    # ── Concept of Home ──────────────────────────────────────────────────
    "Home for me is where I feel safe and truly myself. It is the "
    "place I return to in my mind when the world feels too big.",

    "I have moved many times in my life, but the meaning of home — "
    "that deep yearning for a place to call my own — never fades.",
]
