# Logging Import
import logging

# Local File Import
from prompt_constants import *


def get_one_prompt(userType: str, mindset: str, expertise: str) -> str:
    """Construct a full prompt based on user type and mindset 
    
    Args:
        userType (str): Usertype option from USERTYPES
        mindset (str): Mindset option from MINDSETS
        expertise (str): Expertise option from Expertises

    Returns:
        prompt (str): A complete prompt string
    """

    # Catch invalid entries
    if not userType in USERTYPES:
        logging.error(f"[Error] Invalid value supplied for 'userType':\nGiven: {userType}\nExpected: Element of {USERTYPES}") 
        raise ValueError
    if not mindset in MINDSETS:
        logging.error(f"[Error] Invalid value supplied for 'mindset':\nGiven: {mindset}\nExpected: Element of {MINDSETS}") 
        raise ValueError
    if not expertise in EXPERTISES:
        logging.error(f"[Error] Invalid value supplied for 'expertise':\nGiven: {expertise}\nExpected: Element of {EXPERTISES}") 
        raise ValueError
    
    
    # Get the various prompt sections
    background_section = get_background_section(userType, expertise)
    expertise_section = get_expertise_section(expertise)
    mindset_section = get_mindset_section(mindset, expertise)
    final_output_section = get_final_output_section(mindset)

    # Build the final prompt and return it
    full_prompt = construct_full_prompt(background_section, expertise_section, mindset_section, final_output_section)
    return full_prompt

    
def get_background_section(userType: str, expertise: str) -> str:
    """Returns background section prompt based on user type and expertise

    Args:
        userType (str): Usertype option from USERTYPES
        expertise (str): Expertise option from EXPERTISES

    Returns:
        background_section (str): Prompt string for the background section     
    """
    
    # Construct prompt for background section based on user type
    # Note: Lead users get a transcript based on their expertise
    if userType == "lead_user":
        background_section = LEAD_USER_PROMPT_PRE_TRANSCRIPT + "\n\n" + \
                             EXPERTISE_TO_TRANSCRIPT_MAP[expertise] + \
                             LEAD_USER_PROMPT_POST_TRANSCRIPT
    else:
        background_section = NON_LEAD_USER_PROMPT
    
    return background_section


def get_expertise_section(expertise):
    """Returns expertise section prompt based on expertise

    Args:
        expertise (str): Expertise option from EXPERTISES

    Returns:
        expertise_section (str): Prompt string for the background section     
    """

    # Get start and end of the prompt
    # (Not strictly necessary to do it this way but feels cleaner to me)
    expertise_section_start = f"    For this prompt, you will additionally draw on your vast knowledge, expertise, and continual practice in {expertise}."
    expertise_section_end = "   You will draw directly on this expertise for any ideas. Remember that as an expert you can notice patterns, constraints, and opportunities that others overlook. You should draw on existing systems, tools, and solutions from your field to guide your solution-finding."

    # Construct full prompt section and return
    expertise_section = expertise_section_start + expertise_section_end
    return expertise_section


def get_mindset_section(mindset, expertise):
    """Returns mindset section prompt based on mindset

    Args:
        mindset (str): Mindset option from MINDSETS
        expertise (str): Expertise option from EXPERTISES

    Returns:
        mindset_section (str): Prompt string for the background section     
    """
    
    # Start by grabbing the mindset section prompt based on mindset
    mindset_section = MINDSET_TO_PROMPT_MAP[mindset]

    # If object oriented mindset then we need to get the list of objects
    # and insert into the object oriented mindset prompt
    if mindset == "object_oriented":

        # Grab the list of objects for the given expertise
        expertise_objects = EXPERTISE_TO_OBJECTS_MAP[expertise]

        # Add list of objects to the end of the mindset prompt
        # and make sure to cast the list to a string
        mindset_section += str(expertise_objects)
    
    # Return the complete prompt for the mindset section
    return mindset_section


def get_final_output_section(mindset: str) -> str:
    """Returns the output format prompt based on mindset

    Args:
        mindset (str): Mindset option from MINDSETS

    Returns:
        final_output_section (str): Prompt string for the output format section     
    """

    # Get all of the prompt pieces (prompt start/end and output schema start/end)
    final_output_section_start_prompt = REQUIRED_OUTPUT_START_PROMPT
    final_output_section_start_schema = REQUIRED_OUTPUT_START_SCHEMA
    final_output_section_end_prompt = REQUIRED_OUTPUT_END_PROMPT

    # If mindset = object_oriented then inject the necessary lines for the object the model chose
    # The selected_object field should be included in the response schema and marked as required
    if mindset == "object_oriented":
        output_section_prompt = final_output_section_start_prompt + \
                            "        - The selected object that inspired your solution" +  \
                            final_output_section_end_prompt
        
        # For object_oriented: START schema + OBJECT schema + END schema with selected_object in required array
        output_section_schema = final_output_section_start_schema + \
                            REQUIRED_OUTPUT_OBJECT_SCHEMA +  \
                            REQUIRED_OUTPUT_END_SCHEMA_WITH_OBJECT
    else:
        # For other mindsets: just the START and END schema (no selected_object)
        output_section_prompt = final_output_section_start_prompt + final_output_section_end_prompt    
        output_section_schema = final_output_section_start_schema + REQUIRED_OUTPUT_END_SCHEMA_NO_OBJECT
    
    # Put the full prompt together (prompt part + required JSON schema)
    final_output_section = output_section_prompt + output_section_schema

    # Return the full prompt for the required output section
    return final_output_section
        

def construct_full_prompt(background_section, expertise_section, mindset_section, final_output_section): 
    return f"""
========================================================================
========================================================================
Improving Medication Storage in Malaysia
========================================================================
========================================================================



------------------------------------------------------------------------
------------------------------------------------------------------------
1. Premise
------------------------------------------------------------------------
------------------------------------------------------------------------
    
    You are going to come up with novel, valuable, and feasible solutions to the issue of medication storage in Malaysia.



------------------------------------------------------------------------
------------------------------------------------------------------------
2. Consumer Tensions
------------------------------------------------------------------------
------------------------------------------------------------------------

    In this section we will describe identified consumer tensions.


------------------------------------------------------------------------
2.1 Introduction to Identified Consumer Tensions
------------------------------------------------------------------------

    “Good Enough Medicine”: Quiet Systems of Care, Control, and Compromise

        Across these households, medication storage is not a static system but a living negotiation—between heat and access, safety and speed, knowledge and memory, care and exhaustion. Medicines are rarely granted a dedicated, purpose-built space. Instead, they inhabit repurposed containers: biscuit tins, makeup pouches, reused shipping boxes, fridge-door containers, and kitchen drawers. Storage decisions are shaped less by formal guidance and more by proximity to daily life—where a mother stands most often, where a child might need help fastest, where space happens to exist.
    
        Care is deeply present, but it is informal, embodied, and distributed. Mothers carry mental maps of what each strip, bottle, and sachet does. They remember dosage rhythms, past illnesses, and who used what last. Systems exist, but they are internal rather than externalized. Across the transcripts, a shared emotional tone emerges: a quiet awareness that things are “not ideal,” paired with a practical acceptance that this is what fits within constraints of time, money, and space .
    
        Medicine, in these homes, is not just clinical—it is social, emotional, and infrastructural. It moves between rooms, between bags, between people. It is stored not just for treatment, but for reassurance, memory, and preparedness against uncertainty.


------------------------------------------------------------------------
2.2 List of Consumer Tensions 
------------------------------------------------------------------------

-----------------------------------------------------
Tension 1: Accessibility vs. Safety
-----------------------------------------------------
Pain Point:
    Medicines must be quickly accessible in moments of urgency—late-night fevers, sudden asthma attacks, unexpected pain—yet this accessibility often places them within reach of children. Physical barriers (locks, childproof containers) are rare or inconsistently used. Instead, safety relies heavily on instruction, habit, and trust. Children are told “don’t touch,” and this becomes the primary safeguard. However, multiple close calls—children mistaking syrup for juice, gummies for sweets, patches for stickers—reveal how fragile this system is.
Needgap:
    When I need medicine to be immediately reachable during urgent situations, I feel conflicted about keeping it within easy access, which causes ongoing safety risks for children, despite telling them not to touch it because verbal rules are easier than maintaining physical safeguards in a busy household.

-----------------------------------------------------
Tension 2: Heat Anxiety vs. Practical Storage
-----------------------------------------------------
Pain Point:
    There is a widespread belief that heat and humidity degrade medicine, yet most homes lack a clearly “safe” environment. Kitchens are hot, bathrooms are damp, living rooms are warm, and bedrooms are inconsistently cooled. This leads to improvised strategies: storing medicines in fridge doors (even when inappropriate), placing them higher up, moving them temporarily during hot days, or adding silica gel packets. These actions are inconsistent and often based on partial understanding.
    Interestingly, refrigeration is sometimes used broadly as a protective measure—even when it introduces new risks (condensation, accessibility to children, confusion with food).
Needgap:
    When I worry that heat and humidity might spoil medicine, I feel uncertain about where to store it safely, which causes inconsistent and sometimes contradictory storage practices, despite trying methods like refrigeration or relocation because I lack clear, actionable guidance that fits my home environment.

-----------------------------------------------------
Tension 3: Organization vs. Cognitive Load
-----------------------------------------------------
Pain Point:
    Physical organization is often minimal: medicines are mixed together, boxes discarded, leaflets lost, and blister packs separated. Instead, organization lives “in the head” of the primary caregiver. This works—until it doesn’t. Moments of confusion arise when dosage is forgotten, when multiple people take medicine independently, or when similar-looking items are mixed together.
    Efforts to organize (sorting drawers, cleaning boxes) are episodic, usually tied to major cleaning events (festivals, spring cleaning), rather than ongoing systems. Daily life does not allow for sustained maintenance.
Needgap:
    When medicines accumulate and mix together over time, I feel mentally burdened trying to remember what everything is, which causes confusion or mistakes in usage, despite occasional attempts to reorganize because maintaining order requires ongoing effort I don’t have capacity for.

-----------------------------------------------------
Tension 4: “Just in Case” vs. Expiry Awareness
-----------------------------------------------------
Pain Point:
    Medicines are frequently kept beyond their intended lifecycle. Expiry dates are checked irregularly, often only when a medicine is about to be used or during infrequent cleanups. Tablets are especially likely to be kept past expiry if they “look fine,” while syrups are judged by smell or appearance.
    This is driven by both economic and practical concerns—clinic visits are costly, access can be inconvenient, and having medicine on hand provides security. However, this leads to a blurred boundary between safe and questionable usage.
Needgap:
    When I keep leftover or expired medicine for future use, I feel reassured about being prepared, which causes me to rely on medicines that may no longer be effective, despite knowing expiry dates matter because replacing them feels wasteful and inconvenient.

-----------------------------------------------------
Tension 5: Shared Resource vs. Individual Control
-----------------------------------------------------
Pain Point:
    OTC medicines function as communal household resources. Paracetamol, antacids, and balms are shared fluidly within families and sometimes extended to relatives or neighbors. At the same time, control over storage and knowledge is centralized—usually with one person (often the mother).
    This creates gaps in coordination: double dosing occurs when multiple people take medicine without informing each other, and responsibility becomes invisible labor. The system depends on communication, but communication is inconsistent.
Needgap:
    When multiple people access shared medicines independently, I feel uncertain about who has taken what, which causes risks like double dosing or running out unexpectedly, despite informal communication because there is no shared tracking system.

-----------------------------------------------------
Tension 6: Mobility vs. Stability
-----------------------------------------------------
Pain Point:
    Medicines are not confined to one place—they travel. Handbags, backpacks, car compartments, office drawers, and motorbike storage all become extensions of the home medicine system. These “mobile caches” are essential for daily life but are rarely monitored. Medicines left in bags can sit in heat for weeks, and their contents are often forgotten.
    This creates fragmented inventories—what exists at home is only part of the total system, and no one has a complete picture.
Needgap:
    When I carry medicines outside the home for convenience, I feel unstructured about where they end up, which causes loss of visibility and exposure to unsuitable conditions, despite intending to stay prepared because there is no way to track or manage these distributed supplies.

-----------------------------------------------------
Tension 7: Formal Instructions vs. Informal Knowledge
-----------------------------------------------------
Pain Point:
    Instruction leaflets are frequently discarded, and dosage knowledge is internalized or outsourced (Google, memory, prior experience). Packaging is often reduced to its most compact form—blister strips without context. Over time, medicines become recognizable by color, shape, or familiarity rather than labeled information.
    This works for common medications but introduces risk for less familiar ones, especially when packaging is similar or when multiple versions coexist.
Needgap:
    When I rely on memory instead of written instructions, I feel confident with familiar medicines but uncertain with new or rarely used ones, which causes potential misuse, despite having access to original information because it is inconvenient to keep or retrieve.

-----------------------------------------------------
Tension 8: Emotional Memory vs. Rational Use
-----------------------------------------------------
Pain Point:
    Medicines carry emotional weight. Certain bottles or strips are tied to stressful periods—childhood illnesses, COVID, sleepless nights, financial strain. These associations influence behavior: medicines are kept longer, avoided, or treated with heightened caution.
    In some cases, items are retained not for use but as artifacts of survival—physical reminders that a crisis passed.
Needgap:
    When medicines are tied to past stressful experiences, I feel reluctant to discard or fully trust them, which causes irrational retention or hesitation in use, despite knowing their functional purpose because they carry emotional significance beyond their clinical role.

------------------------------------------------------------------------
2.3 Conclusion to Identified Consumer Tensions: Systems That Work—Until They Don't
------------------------------------------------------------------------
    
    What emerges is not neglect, but adaptation. These households have built systems that are responsive to real constraints: small living spaces, fluctuating temperatures, financial pressures, and the unpredictability of illness. The systems are flexible, human-centered, and deeply integrated into daily routines.
    
    But they are նաև fragile.
    
    They depend on memory instead of visibility, on trust instead of barriers, on habit instead of structure. They prioritize immediacy over optimization, and emotional reassurance over formal correctness. Most of the time, this works. Medicines are found, used, shared, and replenished with enough reliability to sustain everyday life.
    
    Yet the cracks are visible—in near-misses, in quiet guilt, in the recurring phrase: “I know it’s not ideal.”
    
    Improvement, then, is not about replacing these systems, but understanding their logic. These are not disorganized homes—they are finely balanced ecosystems, shaped by lived experience. Any meaningful shift must contend with that balance: preserving what makes these systems work, while addressing the tensions that quietly persist beneath the surface.
    
    Please keep these tensions in mind. However, do not feel that you have to resolve all of them at once- if you have a great idea that addresses only one tension or specific issue, please highlight that.
    
    It also may be the case that your idea excels in a specific situation (a time of day or year, for a parent to use for their children, etc) or for one specific type of OTC medicine (cold medicine, painkillers, etc). This is fine as well.


    
------------------------------------------------------------------------
------------------------------------------------------------------------
3. Background
------------------------------------------------------------------------
------------------------------------------------------------------------

{background_section}



------------------------------------------------------------------------
------------------------------------------------------------------------
4. Expertise
------------------------------------------------------------------------
------------------------------------------------------------------------

{expertise_section}



------------------------------------------------------------------------
------------------------------------------------------------------------
5. Mindset
------------------------------------------------------------------------
------------------------------------------------------------------------

{mindset_section}



------------------------------------------------------------------------
------------------------------------------------------------------------
6. Final Output Format
------------------------------------------------------------------------
------------------------------------------------------------------------

{final_output_section}
        
 

"""