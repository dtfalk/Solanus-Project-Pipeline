# Import All Transcripts
from transcripts import *

# ===================================================================
# General Lists of Variable Possibilities
# ===================================================================

# Types of users
USERTYPES = ["lead_user", "non_lead_user"]

# Problem-solving mindsets
MINDSETS = ["analytical", "insight", "object_oriented"]

# Areas of expertise
EXPERTISES = ["Medicine Packaging", "Industrial Design","Product Design", 
            "Packaging Design", "Manufacturing", "User Experience Research",
            "Pharmacy", "Graphic Design", "Industrial Engineering", 
            "Material Culture Studies", "Advertising", "Circular Economy",
            "History of Medicine", "Particle Physics", "Cultural Anthropology", 
            "Cognitive Science", "Philosophy", "Geology", "Dock Logistics", 
            "HVAC Design", "Astronautics", "Ultra-Endurance Desert Racing",
            "Preschool Education", "Molecular Gastronomy"]
# ===================================================================
# ===================================================================





# ===================================================================
# Background Section Prompt Constants
# ===================================================================

# -------------------------------------------------------------------
# Lead User Prompt Parts
# -------------------------------------------------------------------

LEAD_USER_PROMPT_PRE_TRANSCRIPT = """
Your Background:
    You are Malaysian, and are personally confronted with tensions and difficulties in your system of use for medicine in your daily life. They may relate to some of the tensions listed above. You may however note tensions that go beyond the ones listed. Here, in brief, are some details about your background and about how you store medicine.
"""

LEAD_USER_PROMPT_POST_TRANSCRIPT = """
    Please consider problems related to medication storage the way they arise for you personally, **specifically related to medicine packaging**. You should think by using your particular environment and circumstances to motivate you to consider what needs are most pressing and what solutions would be most useful. 

    To help you, you may want to consider what kinds of ideas and thoughts you have while managing medication storage. Please take your lifestyle and your personal goals into consideration as you innovate.

    The idea you have should **represent a change to existing medication packaging**. This new type of packaging should not be something available to you on the current market, and it should significantly improve your storage outcomes. It should be highly novel and valuable for you as a consumer if a business were to adopt your idea. However, it should also be economically feasible to sell and produce.
"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Non Lead User Prompt
# -------------------------------------------------------------------

NON_LEAD_USER_PROMPT = """
Your Background:
    You work for an over-the-counter medicine company and need to design a new **type of packaging**. To do this, please reference the tensions above to inform what needs are most pressing and what solutions would be most useful to a consumer base. You are guided also by your company’s interests as well. 

    Please take your company's advantages in efficiencies related to specialization and economies of scale into consideration as you innovate. 

    The idea you have should **represent a change to existing medication packaging**. This new type of packaging should not be an existing product, but rather something that does not exist in the current market. It should significantly improve storage outcomes for most consumers in Malaysia. It should be a highly novel and highly valuable idea for the consumer as well as the business. However, it should also be economically feasible to sell and produce.
"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# ===================================================================
# ===================================================================





# ===================================================================
# Minset Section Prompt Constants
# ===================================================================

# -------------------------------------------------------------------
# Analytical Mindset
# -------------------------------------------------------------------
ANALYTICAL_MINDSET_PROMPT_SECTION = """    You should approach solution-finding using analytical, step-by-step problem-solving. Talk about how you came up with your ideas by thinking with this lens.

This means:
    - You define the problem clearly and break it into smaller parts
    - You evaluate constraints and requirements explicitly
    - You search for solutions by incrementally improving or recombining known approaches
    - You prioritize feasibility, reliability, and clarity

You pay attention:
    - To functional requirements (safety, storage conditions, accessibility)
    - To tradeoffs and constraints
    - To known solutions that can be improved

Please use this mindset to arrive at a solution.
"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# Analytical Mindset
# -------------------------------------------------------------------
INSIGHT_MINDSET_PROMPT_SECTION = """    You should approach solution-finding using insight problem-solving. Talk about how you came up with your ideas by thinking with this lens.


This means: 
    - You feel stuck or constrained by initial ways of thinking 
    - You have pulled back from a known problem and have just let the problem “sit”
    - You are open to unexpected connections or shifts in perspective 

You subconsciously pay attention: 
    - To aspects of the problem that don’t quite fit
    - To unusual or distant associations 
    - To moments where the problem suddenly looks different. 

Please use this mindset to arrive at a solution.
"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# Object Oriented Mindset
# -------------------------------------------------------------------
OBJECT_ORIENTED_MINDSET_PROMPT_SECTION = """    You should approach solution-finding as arising from object based reasoning. Talk about how you came up with your ideas by thinking with this lens.

This means: 
    - You have a sense that solutions (in whole or in part) may already exist in structured things in the world such as physical objects, systems, procedures, ideas, or patterns.
    - You treat objects as solution carriers
    - Just like someone finding a puzzle piece knows there is a corresponding puzzle to be solved, you look at objects and infer that they in whole or part are solutions to a problem you didn’t know about.

You pay attention: 
    - To objects to consider their affordances out of high curiosity
    - To the form of physical objects, systems, procedures, ideas, or patterns
    - To solutions that reveal themselves by your consideration of objects (conscious or unconscious). 

Choose one object from the following list as a tool or inspiration for your solution, or select something specific from your immediate environment or field of expertise. Strongly consider each option as a possibility before making your decision.

Here is the list of objects that you may choose from:

"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# Mapping between mindset strings and mindset prompts  
# -------------------------------------------------------------------
MINDSET_TO_PROMPT_MAP = {
    "analytical": ANALYTICAL_MINDSET_PROMPT_SECTION,
    "insight": INSIGHT_MINDSET_PROMPT_SECTION,
    "object_oriented": OBJECT_ORIENTED_MINDSET_PROMPT_SECTION
}
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# ===================================================================
# ===================================================================




# ===================================================================
# Output Section Prompt Variables
# ===================================================================

# -------------------------------------------------------------------
# Output prompts up to where we optionally include selected object
# -------------------------------------------------------------------
REQUIRED_OUTPUT_START_PROMPT = """   Keep in mind who you are and your mindset. The goal is to find novel, valuable, and feasible ideas that can improve medication storage in Malaysia.

    Once again, it is okay if your idea only helps solve one specific problem. For example, it might have a specialized use case (ex. “cold medicine for children” or “for carrying in your bag”). It might address only one of many tensions faced by consumers. This is perfectly fine: focus on the best and most unique part of your idea.

    Your idea must include, in this order:
        - Idea Name
        - Your background
        - Your expertise
        - Your mindset

    IMPORTANT: Each field in your response must contain fully developed, multi-sentence content. Be verbose. Do not provide minimal, bullet-point, or abbreviated responses. Write in complete, detailed paragraphs. Treat each field as an opportunity to fully explain your thinking.
"""

REQUIRED_OUTPUT_START_SCHEMA = """{
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "idea_name": {
      "type": "string",
      "description": "A concise, memorable name for the innovation idea"
    },
    "background": {
      "type": "string",
      "description": "A detailed description of your background and context. Explain your user type, personal situation, or professional role, and how it connects to the problem of medication storage. Write at least several sentences."
    },
    "expertise": {
      "type": "string",
      "description": "A detailed explanation of your area of expertise and how it specifically informs and shapes this solution. Describe what knowledge, skills, or professional experience you are drawing on. Write at least several sentences."
    },
    "mindset": {
      "type": "string",
      "description": "A detailed account of the problem-solving mindset you used (analytical, insight, or object_oriented). Explain how this mindset guided your thinking process and led you to this particular solution. Write at least several sentences."
    },"""

# -------------------------------------------------------------------
# Output prompts after where we optionally include selected object
# -------------------------------------------------------------------
REQUIRED_OUTPUT_END_PROMPT = """        - A detailed overview of the idea: what it is, how it works, why it is valuable, and what problem it solves. Be verbose and write at least several sentences.
        - A comprehensive, verbose description of your solution, including:
            - How your solution improves on our current medication storage methods.
            - The feasibility for producing your solution at a justifiable cost.
            - Any technical notes needed in order to begin to build or actualize your solution.
            - Anything else you feel would be relevant or valuable to include about your solution.
            Write at least several detailed paragraphs for this field. Be as thorough and verbose as possible.
"""

# -------------------------------------------------------------------
# Schema for the selected_object field (only used when mindset is object_oriented)
# -------------------------------------------------------------------
REQUIRED_OUTPUT_OBJECT_SCHEMA = """{
    "selected_object": {
      "type": "string",
      "description": "The object that inspired your solution from the provided list"
    },"""

# -------------------------------------------------------------------
# Two versions of the END schema: one with selected_object in required, one without
# -------------------------------------------------------------------
REQUIRED_OUTPUT_END_SCHEMA_NO_OBJECT = """{
    "detailed_idea_overview": {
      "type": "string",
      "description": "A detailed, verbose overview of the idea: what it is, how it works, why it is valuable, and what problem it solves. Write at least several sentences."
    },
    "comprehensive_solution_description": {
      "type": "string",
      "description": "A comprehensive, verbose description of your solution. Must include: how it improves medication storage, feasibility analysis, technical implementation notes, and any other relevant details. Write at least several detailed paragraphs."
    }
  },
  "required": ["idea_name", "background", "expertise", "mindset", "detailed_idea_overview", "comprehensive_solution_description"]
}"""

REQUIRED_OUTPUT_END_SCHEMA_WITH_OBJECT = """{
    "detailed_idea_overview": {
      "type": "string",
      "description": "A detailed, verbose overview of the idea: what it is, how it works, why it is valuable, and what problem it solves. Write at least several sentences."
    },
    "comprehensive_solution_description": {
      "type": "string",
      "description": "A comprehensive, verbose description of your solution. Must include: how it improves medication storage, feasibility analysis, technical implementation notes, and any other relevant details. Write at least several detailed paragraphs."
    }
  },
  "required": ["idea_name", "background", "expertise", "mindset", "selected_object", "detailed_idea_overview", "comprehensive_solution_description"]
}"""

# For backward compatibility during transition, default END_SCHEMA to the no-object version
REQUIRED_OUTPUT_END_SCHEMA = REQUIRED_OUTPUT_END_SCHEMA_NO_OBJECT
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# ===================================================================
# ===================================================================





# ===================================================================
# Maps From Expertises to Variables of Interest  
# ===================================================================

# -------------------------------------------------------------------
# Mapping from expertises to lists of objects
# -------------------------------------------------------------------

EXPERTISE_TO_OBJECTS_MAP = {
    "Medicine Packaging": ["Salvinia Leaf Air-Retention Hairs", "Vacuum Pump", "Kaleidoscope"],
    "Industrial Design": ["Bat Echolocation", "Gearbox Differential", "Lava Lamp"],
    "Product Design": ["Mangrove Root Filtration Network", "Laser Interferometer", "Wind Chimes"],
    "Packaging Design": ["Spider Silk", "Pneumatic Cylinder", "Dreamcatcher"],
    "Manufacturing": ["Rattan Vine", "Plastic Injection Mold", "Accordion"],
    "User Experience Research": ["Mussel Byssus Adhesion", "GPS Atomic Clock", "Magic 8-Ball"],
    "Pharmacy": ["Banana Leaf Water Channeling", "Ball Bearing", "Glow Stick"],
    "Graphic Design": ["Pinecone Hygromorphic Opening", "Electrostatic Precipitator", "Etch-A-Sketch"],
    "Industrial Engineering": ["Coconut Husk", "Turbine Blade", "Newton's Cradle"],
    "Material Culture Studies": ["Swiftlet Nest Adhesion", "Mechanical Escapement", "Bobblehead Figurine"],
    "Advertising": ["Weaver Ant Chain Formation", "Linear Actuator", "Origami"],
    "Circular Economy": ["Shark Skin Riblets", "Wind Tunnel", "Slinky"],
    "History of Medicine": ["Owl Wing", "Industrial Conveyor Belt", "Rain Stick"],
    "Particle Physics": ["Coral Porous Skeleton", "Particle Accelerator Magnet", "Shadow Puppet Screen"],
    "Cultural Anthropology": ["Hornbill Beak Lightweight Structure", "Cryogenic Storage Tank", "Snow Globe"],
    "Cognitive Science": ["Snail Shell Spiral Growth", "Hydraulic Press", "Juggling Balls"],
    "Philosophy": ["Mangrove Tree Bark Water Shedding", "Ultrasonic Cleaner", "Whoopee Cushion"],
    "Geology": ["Cactus Spine Fog Collection", "Nuclear Reactor Control Rod", "Pinwheel"],
    "Dock Logistics": ["Pitcher Plant Digestive Fluid Stability", "Industrial Heat Exchanger", "Sand Timer"],
    "HVAC Design": ["Electric Eel Bioelectric Generation", "Desalination Reverse-Osmosis Pump", "Hand Warmer Packet"],
    "Astronautics": ["Tree Frog Toe Pad", "MEMS Accelerometer", "Periscope Toy"],
    "Ultra-Endurance Desert Racing": ["Butterfly Wing Structural Color", "Graphene Supercapacitor", "Mood Ring"],
    "Preschool Education": ["Termite Mound Ventilation", "Diesel Fuel Injector", "Slingshot"],
    "Molecular Gastronomy": ["Lotus Leaf Self-Cleaning Surface", "Semiconductor Lithography Machine", "Ouija Board"]
}

# Mapping from expertises to transcriptions
EXPERTISE_TO_TRANSCRIPT_MAP = {
    "Medicine Packaging": MEDICINE_PACKAGING_TRANSCRIPT,
    "Industrial Design": INDUSTRIAL_DESIGN_TRANSCRIPT,
    "Product Design": PRODUCT_DESIGN_TRANSCRIPT,
    "Packaging Design": PACKAGING_DESIGN_TRANSCRIPT,
    "Manufacturing": MANUFACTURING_TRANSCRIPT,
    "User Experience Research": USER_EXPERIENCE_RESEARCH_TRANSCRIPT,
    "Pharmacy": PHARMACY_TRANSCRIPT,
    "Graphic Design": GRAPHIC_DESIGN_TRANSCRIPT,
    "Industrial Engineering": INDUSTRIAL_ENGINEERING_TRANSCRIPT,
    "Material Culture Studies": MATERIAL_CULTURE_STUDIES_TRANSCRIPT,
    "Advertising": ADVERTISING_TRANSCRIPT,
    "Circular Economy": CIRCULAR_ECONOMY_TRANSCRIPT,
    "History of Medicine": HISTORY_OF_MEDICINE_TRANSCRIPT,
    "Particle Physics": PARTICLE_PHYSICS_TRANSCRIPT,
    "Cultural Anthropology": CULTURAL_ANTHROPOLOGY_TRANSCRIPT,
    "Cognitive Science": COGNITIVE_SCIENCE_TRANSCRIPT,
    "Philosophy": PHILOSOPHY_TRANSCRIPT,
    "Geology": GEOLOGY_TRANSCRIPT,
    "Dock Logistics": DOCK_LOGISTICS_TRANSCRIPT,
    "HVAC Design": HVAC_DESIGN_TRANSCRIPT,
    "Astronautics": ASTRONAUTICS_TRANSCRIPT,
    "Ultra-Endurance Desert Racing": ULTRA_ENDURANCE_DESERT_RACING_TRANSCRIPT,
    "Preschool Education": PRESCHOOL_EDUCATION_TRANSCRIPT,
    "Molecular Gastronomy": MOLECULAR_GASTRONOMY_TRANSCRIPT
}
# -------------------------------------------------------------------
# -------------------------------------------------------------------
# ===================================================================
# ===================================================================