#!/bin/bash
#
# Training Data Generation Pipeline
#
# Generates comprehensive training data for BGE-M3 fine-tuning
#
# Usage:
#   ./run-pipeline.sh [stage]
#
# Stages:
#   all         - Run full pipeline (default)
#   negatives   - Mine hard negatives only
#   synthetic   - Generate synthetic queries only
#   paraphrases - Generate Arabic paraphrases only
#   combine     - Combine and validate only
#   pilot       - Run pilot tests for all stages
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prereqs() {
    log_info "Checking prerequisites..."

    # Check bun
    if ! command -v bun &> /dev/null; then
        log_error "bun is not installed. Install from https://bun.sh"
        exit 1
    fi

    # Check OPENROUTER_API_KEY
    if [ -z "$OPENROUTER_API_KEY" ]; then
        log_error "OPENROUTER_API_KEY not set"
        log_info "Export it: export OPENROUTER_API_KEY='sk-or-v1-...'"
        exit 1
    fi

    # Check DATABASE_URL
    if [ -z "$DATABASE_URL" ]; then
        log_warn "DATABASE_URL not set - synthetic queries and paraphrases will fail"
        log_info "Set it: export DATABASE_URL='postgresql://...'"
    fi

    # Check Qdrant
    if ! curl -s http://localhost:6333/health &> /dev/null; then
        log_warn "Qdrant not available at localhost:6333 - hard negative mining will fail"
    fi

    log_success "Prerequisites checked"
}

# Stage 1: Mine hard negatives
run_negatives() {
    log_info "Stage 1: Mining hard negatives..."

    if [ "$1" == "pilot" ]; then
        log_info "Running pilot (1000 pairs)..."
        bun run training/scripts/mine-hard-negatives.ts --limit=1000
    else
        bun run training/scripts/mine-hard-negatives.ts
    fi

    log_success "Hard negatives complete"
}

# Stage 2: Generate synthetic queries
run_synthetic() {
    log_info "Stage 2: Generating synthetic queries..."

    if [ "$1" == "pilot" ]; then
        log_info "Running pilot (1000 passages)..."
        bun run training/scripts/generate-synthetic-queries.ts --pilot
    else
        bun run training/scripts/generate-synthetic-queries.ts
    fi

    log_success "Synthetic queries complete"
}

# Stage 3: Generate Arabic paraphrases
run_paraphrases() {
    log_info "Stage 3: Generating Arabic paraphrases..."

    if [ "$1" == "pilot" ]; then
        log_info "Running pilot (500 passages)..."
        bun run training/scripts/generate-arabic-paraphrases.ts --pilot
    else
        bun run training/scripts/generate-arabic-paraphrases.ts
    fi

    log_success "Arabic paraphrases complete"
}

# Stage 4: Combine and validate
run_combine() {
    log_info "Stage 4: Combining training data..."

    bun run training/scripts/combine-training-data.ts

    log_info "Validating combined data..."
    bun run training/scripts/validate-training-data.ts --input=training/data/combined_training.jsonl

    log_success "Training data ready at training/data/combined_training.jsonl"
}

# Show usage
show_usage() {
    echo "Usage: $0 [stage]"
    echo ""
    echo "Stages:"
    echo "  all         - Run full pipeline (default)"
    echo "  negatives   - Mine hard negatives only"
    echo "  synthetic   - Generate synthetic queries only"
    echo "  paraphrases - Generate Arabic paraphrases only"
    echo "  combine     - Combine and validate only"
    echo "  pilot       - Run pilot tests for all stages"
    echo ""
    echo "Environment variables required:"
    echo "  OPENROUTER_API_KEY - For LLM and Gemini embeddings"
    echo "  DATABASE_URL       - For fetching passages (synthetic/paraphrases)"
    echo ""
    echo "Optional:"
    echo "  QDRANT_URL - Qdrant server (default: http://localhost:6333)"
}

# Main
STAGE="${1:-all}"

case "$STAGE" in
    all)
        check_prereqs
        echo ""
        log_info "Running full pipeline..."
        echo "============================================================"
        run_negatives
        echo ""
        run_synthetic
        echo ""
        run_paraphrases
        echo ""
        run_combine
        echo ""
        echo "============================================================"
        log_success "Pipeline complete!"
        log_info "Next: Upload training/data/combined_training.jsonl to Colab"
        ;;
    negatives)
        check_prereqs
        run_negatives
        ;;
    synthetic)
        check_prereqs
        run_synthetic
        ;;
    paraphrases)
        check_prereqs
        run_paraphrases
        ;;
    combine)
        run_combine
        ;;
    pilot)
        check_prereqs
        echo ""
        log_info "Running pilot tests (small samples to validate quality)..."
        echo "============================================================"
        run_negatives pilot
        echo ""
        run_synthetic pilot
        echo ""
        run_paraphrases pilot
        echo ""
        run_combine
        echo ""
        echo "============================================================"
        log_success "Pilot complete!"
        log_info "Review the outputs before running full pipeline"
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        log_error "Unknown stage: $STAGE"
        show_usage
        exit 1
        ;;
esac
