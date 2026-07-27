"""
Script to train pixel-space class-conditional DDPM using UNet2DModel on the EMNIST dataset
- Author: David Falk
- Organization: APEX Laboratory at The University of Chicago
- Date: 2/11/2026
==================================
Model Training Features:

1. Balanced split (equal samples per class since given dataset class sizes are heavily skewed towars digits)
2. Cosine beta schedule (better fine details)
3. EMA weights (smoother, more stable)
4. CFG dropout (enables guidance at generation time)

Important Notes:
1. I merged letters whose uppercase/lowercase forms are identical/highly-similar (e.g. 's', 'c')
    so that, intuitively speaking, the model doesn't try to learn/internalize some arbitrary difference
    between 's' and 'S'. For inference I have made it so that you can enter either 's' or 'S' and it 
    will cast to the correct class label.
"""

# ==========================================================================================================
# IMPORTS (check conda yml for versions)
# ==========================================================================================================
import os
import time
from pathlib import Path

import torch
import torch.nn.functional as F
from torch.utils.data.distributed import DistributedSampler
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from diffusers import UNet2DModel, DDPMScheduler

# Metrics & logging (factored out — see metrics/ and pretty_logger.py)
from metrics import MetricsTracker, generate_all_plots, generate_report
from metrics.tracker import compute_grad_norm
from pretty_logger import PrettyLogger

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# TRAINING CONFIGS
# ==========================================================================================================

# What to name the model (relevant for save directory)
MODEL_NAME = "balanced_cfg_cosine_ema_600_steps" 

# Choice of model size from SIZES dict found below 
# Controls image resolution, batch size, and channel size/shape
SIZE_NAME = "large"

# Equalizes equal number of samples in each class
# Equal classes helps ensure that no class dominates the loss function
DATASET_SPLIT = "balanced" 

# Number of training epochs (full passes through the dataset)
EPOCHS = 100

# Learning Rate (size of gradient update)
LEARNING_RATE = 1e-4

# Number of diffusion timesteps
# Gaussian noise is iteratively added to original image over {STEPS} timesteps
STEPS = 600

# Controls how noise increases over timesteps
# Each injection of gaussian noise has variance Beta_t
# This controls how Beta_t grows as t grows
BETA_SCHEDULE = "squaredcos_cap_v2"

# Helps smooth weights updates during training
# Instead of doing x, this does y
EMA_DECAY = 0.999

# Enables classifier free gudiance (cfg)
# {CFG_DROPOUT_PROB * 100} percent of the time a class label is replaced with a null label
# Allows the model to run inference (i.e. predict noise) both with and without conditioning
CFG_DROPOUT_PROB = 0.10

# If True, the code will download the EMNIST dataset if missing. 
# Requires internet connection. 
# I reccommend just downloading it ahead of time.
DOWNLOAD_DATASET = False
# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# PATHS (self-contained — everything lives inside the project root)
# ==========================================================================================================

# Project root is one level above this script's directory (emnist-ddpm/)
PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Where to save the trained model/checkpoints
CACHE_ROOT = PROJECT_ROOT / "checkpoints"

# Where the EMNIST dataset folder is stored 
# (downloaded automatically on first run if DOWNLOAD_DATASET == True)
DATASETS_FOLDER = PROJECT_ROOT / "datasets"

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# SIZE PRESETS
# ==========================================================================================================

# Each entry expands into:
#   (img_res, batch_size, block_out_channels)
#
# img_res:
#   Final training image resolution (images resized to img_res x img_res).
#
# batch_size:
#   Number of images processed per training step.
#
# block_out_channels:
#   Tuple defining the number of feature channels at each U Net resolution stage.
#   Each number corresponds to one downsampling stage in the U Net.
#   Each value in the 4-tuple is number of feature maps for that convolution block
#
#   Stage 1:
#       Resolution: img_res x img_res
#       Channels: block_out_channels[0]
#       Learns low-level features (edges, strokes, perfectly aligned with pixel-level structure).
#
#   Stage 2:
#       Resolution: (img_res / 2) x (img_res / 2)
#       Channels: block_out_channels[1]
#       Learns mid-level features (partial shapes).
#
#   Stage 3:
#       Resolution: (img_res / ) x (img_res / 4)
#       Channels: block_out_channels[2]
#       Learns higher-level structure (whole character components).
#
#   Stage 4:
#       Resolution: (img_res / 8) x (img_res / 8)
#       Channels: block_out_channels[3]
#       Bottleneck layer with global context for noise prediction.
#   
# Simplified Explanation For Feature Maps (block_out_channels):
#
# At each stage of the U Net, the model creates multiple "feature maps".
# Think of each feature map as shining a different "pattern revealer" laser
# beam across the image.
#
# Each beam slides over the entire image and lights up wherever its learned
# pattern appears.
#
# The brightness at each pixel answers:
#     "How strongly does this specific pattern exist here?"
#
# The number in block_out_channels tells you how many different pattern
# revealers (feature maps) exist at that stage.
#
# Example:
#     64  →  64 different learned pattern detectors
#     256 →  256 different learned pattern detectors
#
# As the U Net goes deeper:
#     - The image resolution becomes smaller
#     - The number of feature maps increases
#
# Early stages detect simple local patterns (edges, strokes).
# Deeper stages detect more abstract structure (shapes, global layout).
#
# Larger numbers here mean:
#     - More pattern detectors
#     - Greater representational capacity
#     - More parameters and memory usage

# Note:
#   As spatial resolution decreases, channel count increases.
#   This preserves model capacity while controlling memory usage.

SIZES = {
    "small":  (28,  2048, (64, 128, 256)),
    "medium": (64,   384, (64, 128, 256, 256)),
    "large":  (96,   96, (96, 192, 384, 384)),
    "xl":     (128,  32, (128, 256, 512, 512)),
}

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# DISTRIBUTED TRAINING SETUP
# ==========================================================================================================

# Global rank of this process.
# In multi-GPU / multi-node training, each process gets a unique RANK.
# RANK is used to coordinate communication between processes.
# RANK 0 is considered the main process.
# All other ranks will run their own version of this script, but there is logic
# within the imported packages and within our script such that there are some things
# that only the process with rank 0 will do (e.g. logging)
rank = int(os.environ.get("RANK", "0"))

# Local rank of this process on the current machine.
# If a machine has multiple GPUs, LOCAL_RANK tells this process
# which GPU it should use.
local_rank = int(os.environ.get("LOCAL_RANK", "0"))

# Assign this process to its designated GPU.
# Ensures each distributed worker uses a different GPU.
torch.cuda.set_device(local_rank)

# Training device (GPU only currently).
device = "cuda"

# Initialize distributed communication backend so the processes can communicate.
# "nccl" is optimized for GPU-to-GPU communication.
# This enables gradient synchronization across processes.
torch.distributed.init_process_group("nccl")

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# LOAD SIZE PRESET
# ==========================================================================================================

# Unpack selected preset into:
#   img_res   --> image resolution
#   batch --> batch size per process
#   channel_width --> U Net channel widths
img_res, batch, channel_width = SIZES[SIZE_NAME]

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# OUTPUT DIRECTORY
# ==========================================================================================================

# Construct output directory path:
# checkpoints / SIZE_NAME
# Organized by model size for easy export.
OUT_DIR = os.path.join(CACHE_ROOT, SIZE_NAME)

# Create directory if it does not already exist.
os.makedirs(OUT_DIR, exist_ok = True)

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# METRICS & LOGGER SETUP (only rank 0 writes)
# ==========================================================================================================

# Path to folder where we store training metrics and run logs
SCRIPT_DIR = Path(__file__).parent.resolve()
METRICS_DIR = SCRIPT_DIR / "metrics" / "output" / SIZE_NAME
LOG_DIR = SCRIPT_DIR / "logs"

# Make these directories if they do not already exist
METRICS_DIR.mkdir(parents = True, exist_ok = True)
LOG_DIR.mkdir(parents = True, exist_ok = True)

# Set the coordinator GPU (rank == 0) such that it has a tracker/logger
if rank == 0:
    tracker = MetricsTracker(output_dir = METRICS_DIR, model_name = MODEL_NAME)
    logger = PrettyLogger(os.path.join(LOG_DIR, f"train_{SIZE_NAME}.log"))
    logger.start()
    logger.log("Initializing training run...", severity = "ok")
else:
    tracker = None
    logger = None


def log(msg, severity = "info"):
    """Convenience: log from rank 0 only.
        Args:
          msg (str): The message to log
          severity (str): The severity level of the message (info, error, warn, etc...)
    """

    # Only log if it is coordinator GPU
    if rank == 0 and logger is not None:
        logger.log(msg, severity=severity)

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# DATA LOADING AND PREPROCESSING
# ==========================================================================================================

# Log training run metadata
log(f"Model: {MODEL_NAME} | Size: {SIZE_NAME} | Image Resolution: {img_res}x{img_res}", "ok")
log(f"Split: {DATASET_SPLIT} | Epochs: {EPOCHS} | Learning Rate: {LEARNING_RATE}", "ok")
log(f"Schedule: {BETA_SCHEDULE} | EMA: {EMA_DECAY} | CFG Drop: {CFG_DROPOUT_PROB}", "ok")

# Builds the image transformation pipeline using the torchvision.transforms package
# As implied by its name, a pipeline is just a set of composable, modular steps that are glued together
# In this case we ".Compose" (glue together) the following image transformations into a single object (the "image_transformation_pipeline" variable)
# We will apply this deterministic set of operations to every image in the dataset.
# Usage: Load EMNIST dataset images as a PIL images (greyscale) and run "image_transformation_pipeline(pil_image) for each image" 
# 
# For each image, the pipeline does the following:
#   1. Resize the EMNSIT image (28 x 28) to the target resolution (img_res x img_res)   
#   2. Convert the image to a PyTorch Tensor. This changes integer image values into float32 values in the interval [0, 1]
#   3. Rotates the image 90 degrees and flips along height axis so that images are upright (EMNIST images are stored rotated and flipped so we undo this...tbh no clue why)
#   4. Scale the tensors from the interval [0, 1] to the interval [-1 , 1] using f(x) = 2x - 1 (center pixel values around 0)
image_transformation_pipeline = transforms.Compose([
    transforms.Resize((img_res, img_res)),  # Step 1: Resize to img_res
    transforms.ToTensor(),                  # Step 2: Convert to PyTorch Tensor
    transforms.Lambda(lambda x: torch.flip(torch.rot90(x, 1, [1, 2]), [1])), # Step 3: Rotate and flip
    transforms.Lambda(lambda x: x * 2 - 1), # Step 4: scale values from [0, 1] to [-1, 1]
])

# Loads the EMNIST image dataset as a torchvision dataset object
# Basically just a wrapper around the EMNIST files that makes them act like a PyTorch dataset
# There are two internal methods that are of particular interest:
#   1. __len__() --> Returns how many samples exist 
#   2. __getitem__(index) --> Returns a single (image, label) tuple where label is the class name
# __getitem__ will be called repeatedly during the training run. 
dataset = datasets.EMNIST(
    root      = DATASETS_FOLDER,                # Path to EMNIST files
    split     = DATASET_SPLIT,                  # Choose the dataset split (EMNIST allows "balanced", "byclass", "letters", "digits", etc...)
    train     = True,                           # EMNIST has both training and testing partions. "train = True" means use the training partition for training
    download  = DOWNLOAD_DATASET,               # Controls whether TorchVision should download the dataset if it does not exist
    transform = image_transformation_pipeline,  # Applies the image transformation pipeline (not upfront, runs on the fly)
)

# Initializes the distributed sampler
# Loosely, the sampler decides "In what order should the dataset indices be read?"
# Normally, PyTorch either reads samples sequentially or picks randomly (~ shuffle and then read sequentially).
# However, since this is a distributed training setup, we have multiple processes running simultaneously and we 
# need to make sure that we do not have each GPU read the whole dataset/do duplicate training/mess up gradients
# DistributedSampler fixes this by splitting the indices across processes.
# (Also keeps the shuffling synchronized across processes when we call "sampler.set_epoch" later)
sampler = DistributedSampler(dataset)

# Wraps the dataset object in a DataLoader object and gives you an object to iterate over while training.
# Think of DataLoader as a batching and parrallel loading engine.
# It does not change the data in any way.
# It just controls how the data is delivered to the training loop.
data_loader = DataLoader(
    dataset     = dataset, # Provides our dataset object
    batch_size  = batch,   # Number of samples per training step per process
    sampler     = sampler, # Provides our distributed sampler
    num_workers = 8,       # Number of cpu subprocesses used to load & preprocess data in parallel
    pin_memory  = True,    # GPU training optimization (locks CPU memory pages, makes transfer to GPU faster)
    drop_last   = True,    # If dataset is not divisible by batch size, then drop the final, smaller batch 
)                          # (It can be a pain if different GPUs try synchronizing gradients when they have different batch sizes)

# Gets number of classes for the given dataset split
# Valid class labels are ints [0, 1, ..., num_classes - 1]
# We add an EXTRA label which we refer to as NULL_CLASS, which will have index/internal label `num_classes` so there is a cfg dropout class/label
num_classes = len(dataset.classes)
NULL_CLASS = num_classes

# Log class information
log(f"Classes: {num_classes} | Samples: {len(dataset)} | Null class: {NULL_CLASS}", "ok")

# ==========================================================================================================
# ==========================================================================================================



# ==========================================================================================================
# MODEL + EMA (Exponential Moving Average)
# ==========================================================================================================


log("Building UNet...", "info")

# ----------------------------------------------------------------------------------------------------------
# UNet2DModel
# ----------------------------------------------------------------------------------------------------------
#
# This is the core neural network that learns to predict noise.
#
# In diffusion training, we corrupt a clean image x_0 into x_t
# by gradually adding Gaussian noise over many timesteps. 
# Training this model is just teaching it how to start with image x_t
# and remove noise over t timesteps. 
#
# The model learns the function:
#
#     ε_pred = f(x_t, t, class_label)
#
# Where:
#     x_t         = image at timestep t
#     t           = diffusion timestep
#     class_label = conditioning signal (character identity)
#     ε_pred      = predicted noise
#
# The training objective minimizes:
#
#     MSE(ε_pred, ε) where...
#     MSE = (1/n) *     Σ (y_i - y_hat_i)^2
#                   i=1,2,..n 
# where ε is the true Gaussian noise used to corrupt the image.
#
# UNet is a special type of convolutional neural network.
#
# It has three main parts:
#
# 1) Downsampling path (the "compress" part)
#    - The image is gradually reduced in spatial size
#    - Example: 64x64 → 32x32 → 16x16 → 8x8
#    - As the image gets smaller, the number of feature channels increases
#    - The model learns more abstract patterns at each step
#
#    You can think of this as:
#        "Zooming out to understand the overall structure"
#
#
# 2) Bottleneck (the smallest representation)
#    - This is the most compressed version of the image
#    - It contains global information about the character
#    - At this point, the model has a high-level understanding
#
#
# 3) Upsampling path (the "rebuild" part)
#    - The network increases the image size step by step
#    - Example: 8x8 → 16x16 → 32x32 → 64x64
#    - It uses what it learned during compression to reconstruct details
#
#
# Skip connections:
# -----------------
# At every resolution level, the network copies features from the
# downsampling path and sends them directly to the matching
# upsampling stage.
#
# Why?
#
# Because when you compress an image, you lose fine detail.
# Skip connections give the network access to the original
# high-resolution information so it can rebuild sharp results.
#
#
# Why is this good for diffusion?
# --------------------------------
#
# In diffusion training, the model must take a noisy image and
# predict the exact noise that was added.
#
# That requires:
#     - Understanding global structure (what character is this?)
#     - Preserving pixel-level precision (where exactly is the noise?)
#
# The UNet architecture is well suited for this because:
#     - The down path captures overall structure
#     - The up path restores spatial detail
#     - Skip connections prevent loss of fine information
# Note: Only using DownBlock2D and NOT DownBlockAttn bc efficient and probably would be overkill...
#       Should i test this? Idk i'll look into it

unet = UNet2DModel(
    sample_size        = img_res,         # Spatial resolution the UNet is built for (inputs will be B x 1 x img_res x img_res)
    in_channels        = 1,               # Number of channels in the input image (greyscale => 1, RGB => 3)
    out_channels       = 1,               # Number of channels in the output (predict noise; same shape/channels as input)
    layers_per_block   = 2,               # How many conv/resnet layers to run inside each stage before moving to next resolution
    block_out_channels = channel_width,   # Channel widths at each UNet stage (controls capacity/memory; one entry per resolution level)
    down_block_types   = ("DownBlock2D",) * len(channel_width),  # Downsampling path blocks (one per stage; shrink spatial size by 2 each stage)
    up_block_types     = ("UpBlock2D",) * len(channel_width),    # Upsampling path blocks (one per stage; grow spatial size by 2 each stage)
    num_class_embeds   = num_classes + 1, # Size of class embedding table (num classes + 1 extra NULL class for CFG dropout)
).to(device)  # Move model weights to GPU

# Wraps model for multi-GPU training 
# Each process uses its local GPU; gradients sync across ranks
unet = torch.nn.parallel.DistributedDataParallel(unet, device_ids = [local_rank])

# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------



# ----------------------------------------------------------------------------------------------------------
# EMA model (Exponential Moving Average of training weights)
# ----------------------------------------------------------------------------------------------------------
# We create a second copy of the UNet that does NOT train directly.
# Instead, after every optimizer step, we update its weights using:
#
#     ema_weight = EMA_DECAY * ema_weight + (1 - EMA_DECAY) * current_weight
#
# So instead of jumping around like normal SGD updates,
# the EMA model changes slowly and smoothly over time.
#
# Why do this?
#
# During training, model weights fluctuate from step to step.
# Some updates improve things, some slightly overshoot.
#
# The EMA version acts like a "smoothed" version of the model
# across many recent training steps.
#
# Important:
# The optimizer updates the main UNet.
# The EMA UNet is updated manually using the moving average rule.


# EMA model
# (matches initialization for unet)
ema_unet = UNet2DModel(
    sample_size        = img_res,
    in_channels        = 1,
    out_channels       = 1,
    layers_per_block   = 2,
    block_out_channels = channel_width,
    down_block_types   = ("DownBlock2D",) * len(channel_width),
    up_block_types     = ("UpBlock2D",) * len(channel_width),
    num_class_embeds   = num_classes + 1,
).to(device)

# Initialize EMA model with the exact same weights as the training UNet.
# Because unet is wrapped in DistributedDataParallel, the actual model
# lives inside unet.module.
ema_unet.load_state_dict(unet.module.state_dict())

# Put EMA model into evaluation mode.
# We never train this model directly; it behaves like an inference model.
# According to documentation this would be equivalent to `ema_unet.train = False`
ema_unet.eval()

# Disable gradient tracking for EMA parameters.
# EMA weights are NOT updated via backprop or the optimizer.
# They are updated manually using the exponential moving average rule
for p in ema_unet.parameters():
    p.requires_grad_(False)

# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------
# Scheduler, Optimizer & Scaler
# ----------------------------------------------------------------------------------------------------------

# Diffusion scheduler
# Defines the forward diffusion process (adding noise to clean images):
#   - How many timesteps to diffuse over
#   - How noise variance (beta) increases over time
# This object is used to add noise to clean images during training.
scheduler = DDPMScheduler(
    num_train_timesteps = STEPS,          # Total number of diffusion steps (e.g. 600)
    beta_schedule       = BETA_SCHEDULE,  # Controls how noise grows across timesteps (e.g. cosine schedule)
)

# Optimizer
# Updates the training UNet weights using gradients computed from the loss.
# AdamW is commonly used for diffusion models. Not super familiar with other options.
optimizer = torch.optim.AdamW(unet.parameters(), lr = LEARNING_RATE)

# Automatic Mixed Precision (AMP) gradient scaler
# When training in float16, gradients can underflow
# GradScaler dynamically scales the loss to keep training stable
try:
    # Try new pattern
    scaler = torch.amp.GradScaler("cuda") 
    autocast_context = lambda: torch.amp.autocast("cuda", dtype = torch.float16)
except:
    # Fall back to legacy if it fails
    scaler = torch.cuda.amp.GradScaler()
    autocast_context = lambda: torch.cuda.amp.autocast(dtype = torch.float16)

# ----------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------
# ==========================================================================================================
# ==========================================================================================================


# ==========================================================================================================
# TRAINING
# ==========================================================================================================

# Training start
log(f"Starting training — {EPOCHS} epochs on {torch.cuda.device_count()} GPUs", "ok")

# Build config dict for the report
TRAINING_CONFIG = {
    "model_name": MODEL_NAME,
    "size_name": SIZE_NAME,
    "image_resolution": img_res,
    "batch_size_per_gpu": batch,
    "block_out_channels": list(channel_width),
    "dataset_split": DATASET_SPLIT,
    "epochs": EPOCHS,
    "learning_rate": LEARNING_RATE,
    "diffusion_steps": STEPS,
    "beta_schedule": BETA_SCHEDULE,
    "ema_decay": EMA_DECAY,
    "cfg_dropout_prob": CFG_DROPOUT_PROB,
    "num_classes": num_classes,
    "null_class_index": NULL_CLASS,
    "num_gpus": torch.cuda.device_count(),
    "optimizer": "AdamW",
    "mixed_precision": "float16",
}

# One epoch = one full pass through the entire dataset.
# The dataset is divided into mini-batches.
#
# Each mini-batch contains `batch_size` images.
# One mini-batch --> one forward pass --> one loss --> one optimizer step.
for epoch in range(EPOCHS):

    # Important for DistributedSampler.
    # Ensures each GPU shuffles data differently each epoch
    # but in a synchronized way across processes.
    # NOTE: CAN'T I JUST DO THIS IN INITIALIZATION??
    sampler.set_epoch(epoch)

    # Get timestamp for start of current epoch
    t0 = time.time()

    # Start metrics tracking for this epoch
    if rank == 0 and tracker is not None:
        tracker.start_epoch()

    # Store one scalar loss per mini-batch.
    # This is used only for computing the average loss at the end of the epoch.
    losses = []
    batch_idx = 0
    total_batches = len(data_loader)

    # ==================================================================================================
    # MINI-BATCH LOOP
    # ==================================================================================================
    #
    # data_loader yields MINI-BATCHES.
    #
    # images_batch:
    #   Tensor of CLEAN images from the dataset.
    #   Shape: (batch_size, 1, img_res, img_res)
    #
    #   In diffusion notation, this is x_0
    #   (the original, uncorrupted image).
    #
    # labels_batch:
    #   Tensor of integer class labels corresponding to each image.
    #   Shape: (batch_size,)
    #
    #   Each entry tells us what character the image represents.
    #
    # Example:
    #   If batch_size = 384,
    #   Then x contains 384 clean training images,
    #   and y contains 384 class labels.
    #
    for images_batch, labels_batch in data_loader:

        # Load images and their labels to GPU
        images_batch = images_batch.to(device, non_blocking = True)
        labels_batch = labels_batch.to(device, non_blocking = True)

        # -------------------------------------------------------------------------
        # CFG dropout 
        # -------------------------------------------------------------------------
        
        # Create mask for switching CFG_DROPOUT_PROB percent of labels to Null Class
        # Looks like ... dropout_mask = [False, True, False, False, True, ...]
        # All class labels where dropout_mask[i] == True results in that image being trained as 
        # if it is a member of the null class
        dropout_mask = torch.rand(labels_batch.size(0), device = device) < CFG_DROPOUT_PROB

        # Apply the dropout mask so ~CFG_DROPOUT_PROB percent images are now Null Class
        # (Reminder: This is what allows us to do conditional diffusion later if we so desire. 
        # This is where our whole diffusion geometry/force zoo stuff comes from)
        labels_cfg = torch.where(dropout_mask, torch.full_like(labels_batch, NULL_CLASS), labels_batch)
        # -------------------------------------------------------------------------
        # -------------------------------------------------------------------------

        # -------------------------------------------------------------------------
        # Forward Diffusion (Add Noise to Clean Images)
        # -------------------------------------------------------------------------
        #
        # At this point:
        #
        #   images_batch = x_0  (clean training images)
        #   labels_cfg   = class labels (possibly replaced with NULL_CLASS)
        #
        # Diffusion training works by:
        #
        #   1) Sampling a random timestep t
        #   2) Adding the correct amount of Gaussian noise
        #      corresponding to that timestep
        #
        # Each image in the mini-batch receives its OWN timestep.
        #
        # This ensures the model learns to denoise
        # at all possible corruption levels.
        # -------------------------------------------------------------------------
        # -------------------------------------------------------------------------

        # -------------------------------------------------------------------------
        # FORWARD DIFFUSION STEP: Sample Diffusion Timesteps 
        # -------------------------------------------------------------------------
        #
        # Why are we randomly sampling timesteps?
        #
        # In diffusion, the model must learn to denoise images at
        # *every possible noise level*, not just one.
        #
        # If STEPS = 600, then the valid timesteps are:
        #   0, 1, 2, ..., 599
        #
        # Small timestep  → image is lightly noised
        # Large timestep  → image is heavily noised
        #
        # During training, we want the model to learn:
        #
        #   "Given (x_t, t), predict the noise that was added."
        #
        # The true objective is an expectation over all timesteps:
        #
        #   E_t [ MSE(predicted_noise, true_noise) ]
        #
        # Instead of looping over all timesteps sequentially
        # (which would be inefficient),
        # we randomly sample a timestep for each image.
        #
        # This means that within a single mini-batch:
        #   - Some images are lightly corrupted
        #   - Some are moderately corrupted
        #   - Some are heavily corrupted
        #
        # Over many mini-batches, this random sampling
        # approximates the full expectation over timesteps.
        #
        # Intuition:
        # We are training the model to restore images at ALL
        # levels of corruption simultaneously 
        #
        # Important note: 
        #   It may seem strange (and perhaps confuse your mental model to see
        #   that each batch contains images that are at timesteps. In principle,
        #   an image at timestep T has receieved T additions of Gaussian noise.
        #   But it is quite easy to add Gaussian noise T times without adding 
        #   much computational load. That is because T additions of Gaussian 
        #   noise with mean M and variance V (STD = sqrt(V)) is equivalent one 
        #   addition of Gaussian noise with mean T * M and variance T * V.
        #   If we assume M = 0 and V = 1, then this simplifies to one addition 
        #   of Gaussian noise with a mean of 0 and a variance of T.
        # 
        #   In reality diffusion is slightly more complicated because we do NOT add
        #   the same amount of noise at every step. Instead, we use a noise schedule.
        #   The scheduler decides, for each timestep t:
        #
        #       1. how much of the previous image to keep
        #       2. how much fresh Gaussian noise to inject
        #
        #   The default scheduler used here is `squaredcos_cap_v2`, which is a
        #   cosine-based schedule. Intuitively:
        #
        #       - early timesteps add only a little noise
        #       - later timesteps add much more noise
        #
        #   This is usually better than noising too aggressively at the start,
        #   because if we destroy the signal too quickly then later noising steps
        #   are less useful.
        #
        #   To describe one forward diffusion step, define:
        #
        #       1. ε_t  - fresh Gaussian noise sampled at step t
        #                 ε_t ~ N(0, I)
        #
        #       2. β_t  - the fraction of NEW noise injected at step t
        #
        #       3. α_t  - the fraction of the PREVIOUS signal retained at step t
        #                 where α_t = 1 - β_t
        #
        #   One forward diffusion step is then:
        #
        #       x_t = sqrt(α_t) * x_{t-1} + sqrt(1 - α_t) * ε_t
        #
        #   Interpretation:
        #
        #       - sqrt(α_t) scales down the previous image x_{t-1}
        #       - sqrt(1 - α_t) scales the fresh noise ε_t
        #
        #   So x_t is a mixture of:
        #
        #       1. the partially preserved previous image
        #       2. newly injected Gaussian noise
        #
        #   If we repeatedly apply this rule from x_0 to x_t, then after t steps
        #   we get:
        #
        #       x_t = sqrt(alpha_bar_t) * x_0 + sqrt(1 - alpha_bar_t) * ε
        #
        #   where:
        #
        #       alpha_bar_t = α_1 * α_2 * ... * α_t
        #
        #   and ε is a single Gaussian noise tensor representing the COMBINED
        #   effect of all the individual noise injections ε_1, ε_2, ..., ε_t.
        #
        #   This is the key simplification that allows us to calculate in practice:
        #
        #       instead of explicitly simulating every intermediate noising step,
        #       we directly construct x_t in one shot using alpha_bar_t
        #
        #   That is exactly what the scheduler does for us below.
        # 
        #   Source for `squaredcos_cap_v2`: 
        #       Paper: Improved Denoising Diffusion Probabilistic Models (2021)
        #       Authors: Alex Nichol, Prafulla Dhariwal
        #       Link: https://arxiv.org/pdf/2102.09672

        # Generates a batch_size number of random ints
        # Sampled uniformly from low (inclusive) to high (exclusive)
        timesteps_batch = torch.randint(
            low    = 0,
            high   = STEPS,
            size   =(images_batch.size(0),),
            device = device
        )

        # Sample Gaussian noise with the same shape as the images
        # This represents the "true noise" we will ask the model to predict
        noise_batch = torch.randn_like(images_batch)

        # Generate the noisy images x_t using the scheduler
        #
        # Mathematically:
        #            
        #   x_t = sqrt(alpha_bar_t) * x_0 + sqrt(1 - alpha_bar_t) * noise
        #
        # The scheduler handles the correct scaling internally.
        noisy_images_batch = scheduler.add_noise(
            original_samples = images_batch,   # The original images
            noise            = noise_batch,    # The noise to add to each image
            timesteps        = timesteps_batch # The timesteps each image is at, hypothetically
        )
        # -------------------------------------------------------------------------
        # -------------------------------------------------------------------------


        # -------------------------------------------------------------------------
        # Predict Noise with UNet
        # -------------------------------------------------------------------------
        #
        # The model receives:
        #   - noisy_images_batch  (x_t)
        #   - timesteps_batch     (t)
        #   - labels_cfg          (class conditioning signal)
        #
        # It outputs predicted noise for each image.

        # Set to none for speed, mem bandwidth and we're not being fancy here 
        # so let pytorch handle it
        optimizer.zero_grad(set_to_none = True)

    
        
        with autocast_context():
            
        # -------------------------------------------------------------------------
        # Predict Noise with UNet 
        # Big picture: 
        #   1. Predict (this block of code)
        #   2. Measure loss
        #   3. Use gradient descent (optimizer variable) to make the model better
        # -------------------------------------------------------------------------
        #
        # At this point we have:
        #
        #   noisy_images_batch = x_t   (corrupted images at timestep t)
        #   timesteps_batch    = t     (noise level for each image)
        #   labels_cfg         = class label (includes NULL label for CFG dropout)
        #
        # The UNet is trained to solve the following problem:
        #
        #     "Given a noisy image x_t (the clean image with `t` additions of  
        #      gaussian noise), can you recover the exact noise ε
        #      that was added to the original clean image (the `x_0` variable)?"
        #
        # In other words, the model learns the function:
        #
        #     ε_pred = f(x_t, t, class_label)
        #     where x_t = x_0 +
        #
        # Where:
        #   ε_pred is the model's prediction of the Gaussian noise
        #
        # Why predict noise instead of the clean image directly?
        #
        #   - The noise distribution is simple (Gaussian), making it easier to learn
        #   - The same objective works across all timesteps
        #   - Empirically leads to more stable training and better results
        #
        # Internally:
        #   - The UNet processes the noisy image through its downsampling path
        #     (capturing global structure)
        #   - Then reconstructs through the upsampling path (restoring detail)
        #   - Skip connections preserve fine-grained spatial information
        #
        # The output has the SAME shape as the input:
        #
        #   predicted_noise_batch.shape == noisy_images_batch.shape
        #
        # Note:
        #   The UNet returns an object with multiple fields.
        #   `.sample` extracts the predicted noise tensor.
        #
            predicted_noise_batch = unet(
                noisy_images_batch,
                timesteps_batch,
                class_labels=labels_cfg
            ).sample


            # ---------------------------------------------------------------------
            # Loss Computation
            # ---------------------------------------------------------------------
            #
            # We compute Mean Squared Error between:
            #
            #   predicted_noise_batch
            #   noise_batch (true noise)
            #
            # Important:
            #
            # The MSE is averaged across:
            #   - All pixels
            #   - All channels
            #   - All images in the mini-batch
            #
            # The result is ONE scalar value.
            #
            # This scalar answers:
            #
            #   "How wrong was the model on this entire mini-batch?"
            #
            loss = F.mse_loss(predicted_noise_batch, noise_batch)


        # -------------------------------------------------------------------------
        # Backpropagation + Optimizer Step (Mixed Precision)
        # -------------------------------------------------------------------------
        #
        # We trained under torch.cuda.amp.autocast("cuda", float16) or torch.cuda.amp.autocast(float16),
        # which improves speed and reduces memory usage.
        #
        # However, float16 gradients can underflow (become too small to represent).
        #
        # GradScaler solves this by:
        #   1) Scaling the loss upward before backward()
        #   2) Unscaling gradients before optimizer step
        #   3) Dynamically adjusting scaling factor over time
        #
        # This allows stable mixed precision training.
        #
        scaler.scale(loss).backward()     # Compute gradients (scaled)
        scaler.unscale_(optimizer)        # Unscale gradients for accurate norm & clipping
        grad_norm_val = compute_grad_norm(unet.module)
        scaler.step(optimizer)            # Update main UNet weights
        scaler.update()                   # Adjust scaling factor for next step

        # -------------------------------------------------------------------------
        # EMA Update (Exponential Moving Average of Weights)
        # -------------------------------------------------------------------------
        #
        # After the main UNet weights are updated,
        # we update the EMA UNet to be a smoothed version.
        #
        # For each parameter:
        #
        #   ema_weight = EMA_DECAY * ema_weight
        #                + (1 - EMA_DECAY) * current_weight
        #
        # EMA_DECAY is typically close to 1 (e.g. 0.999),
        # meaning:
        #
        #   - The EMA model changes slowly
        #   - It represents a long-term average of recent weights
        #
        # Why do this?
        #
        # During training, weights can fluctuate from mini-batch to mini-batch.
        # EMA smooths those fluctuations.
        #
        # In diffusion models, EMA weights almost always produce:
        #   - More stable generations
        #   - Cleaner samples
        #   - Better generalization
        #
        # Important:
        # The optimizer updates the main UNet.
        # The EMA UNet is NEVER updated by gradients.
        #
        with torch.no_grad():
            for ema_param, current_param in zip(
                ema_unet.parameters(),
                unet.module.parameters()
            ):
                ema_param.mul_(EMA_DECAY).add_(
                    current_param,
                    alpha= 1 - EMA_DECAY
                )


        # Store the scalar loss from this mini-batch.
        # This does NOT affect training.
        # It is only used to compute average loss for the epoch.
        batch_loss = loss.item()
        losses.append(batch_loss)
        batch_idx += 1

        # Record batch metrics + update logger progress
        if rank == 0:
            if tracker is not None:
                tracker.log_batch(loss=batch_loss, batch_size=images_batch.size(0), grad_norm=grad_norm_val)
            if logger is not None:
                logger.set_progress(
                    epoch=epoch,
                    batch=batch_idx,
                    total_batches=total_batches,
                    total_epochs=EPOCHS,
                    current_loss=batch_loss,
                )


    # -------------------------------------------------------------------------
    # End of Epoch: Logging + Checkpointing
    # -------------------------------------------------------------------------

    # Compute the average mini-batch loss across the epoch.
    #
    # This gives a rough measure of training progress.
    # It is not a validation metric, just a training signal.
    mean_loss = sum(losses) / len(losses)

    # Compute how long the epoch took
    dt = time.time() - t0


    # Only the main process (rank 0) logs and saves.
    # In distributed training, every GPU runs this script,
    # so we restrict logging/checkpointing to avoid duplication.
    if rank == 0:

        # Finalize epoch metrics
        epoch_metrics = None
        if tracker is not None:
            epoch_metrics = tracker.end_epoch(
                epoch     = epoch,
                lr        = LEARNING_RATE,
                ema_unet  = ema_unet,
                live_unet = unet.module,
            )

        # Log to pretty logger dashboard
        if logger is not None:
            sps = epoch_metrics.samples_per_sec if epoch_metrics else 0.0
            grad = epoch_metrics.grad_norm if epoch_metrics else 0.0
            gpu_peak = epoch_metrics.gpu_mem_peak_mb if epoch_metrics else 0.0
            ema_d = epoch_metrics.ema_delta_norm if epoch_metrics else 0.0
            logger.log_epoch_summary(
                epoch       = epoch,
                loss        = mean_loss,
                lr          = LEARNING_RATE,
                sps         = sps,
                dt          = dt,
                grad_norm   = grad,
                gpu_peak_mb = gpu_peak,
                ema_delta   = ema_d,
            )

        # Save EMA checkpoint
        torch.save(
            {
                "unet": ema_unet.state_dict(),
                "classes": dataset.classes,
                "scheduler": scheduler.config,
                "size": img_res,
                "channels": channel_width,
                "variation": MODEL_NAME,
                "epoch": epoch,
                "split": DATASET_SPLIT,
                "beta_schedule": BETA_SCHEDULE,
                "ema_decay": EMA_DECAY,
                "cfg_dropout_prob": CFG_DROPOUT_PROB,
                "null_class_index": NULL_CLASS,
                "num_class_embeds": num_classes + 1,
            },
            os.path.join(
                OUT_DIR,
                f"emnist_{MODEL_NAME}_epoch{epoch:03d}.pt"
            ),
        )

# =====================================================
# POST-TRAINING: PLOTS, REPORT, CLEAN SHUTDOWN
# =====================================================
if rank == 0:
    log("Training complete — generating plots and report...", "ok")

    if tracker is not None:
        # Generate all training plots
        try:
            plot_paths = generate_all_plots(tracker)
            log(f"Saved {len(plot_paths)} plots to {METRICS_DIR}/plots", "ok")
        except Exception as e:
            log(f"Plot generation failed: {e}", "warn")
            plot_paths = []

        # Generate Markdown report
        try:
            report_path = generate_report(
                tracker=tracker,
                config=TRAINING_CONFIG,
                plot_paths=plot_paths,
            )
            log(f"Report saved: {report_path}", "ok")
        except Exception as e:
            log(f"Report generation failed: {e}", "warn")

        log(f"CSV metrics: {tracker.csv_path}", "metric")
        log(f"JSON metrics: {tracker.json_path}", "metric")

    log(f"[{MODEL_NAME}] All done.", "ok")

    if logger is not None:
        logger.stop()

torch.distributed.destroy_process_group()
