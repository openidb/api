#!/bin/bash
#
# Check environment for training data generation
#

echo "=============================================="
echo "Training Pipeline Environment Check"
echo "=============================================="
echo ""

# Check bun
echo -n "bun: "
if command -v bun &> /dev/null; then
    echo "✓ $(bun --version)"
else
    echo "✗ NOT INSTALLED"
fi

# Check Python
echo -n "python: "
if command -v python3 &> /dev/null; then
    echo "✓ $(python3 --version 2>&1 | head -1)"
else
    echo "✗ NOT INSTALLED"
fi

# Check OPENROUTER_API_KEY
echo -n "OPENROUTER_API_KEY: "
if [ -n "$OPENROUTER_API_KEY" ]; then
    echo "✓ SET (${#OPENROUTER_API_KEY} chars)"
else
    echo "✗ NOT SET"
    echo "   → export OPENROUTER_API_KEY='sk-or-v1-...'"
fi

# Check DATABASE_URL
echo -n "DATABASE_URL: "
if [ -n "$DATABASE_URL" ]; then
    echo "✓ SET"
else
    echo "✗ NOT SET"
    echo "   → export DATABASE_URL='postgresql://user:pass@host:5432/db'"
fi

# Check PostgreSQL
echo -n "PostgreSQL: "
if pg_isready -h localhost -p 5432 &> /dev/null; then
    echo "✓ RUNNING on localhost:5432"
else
    echo "✗ NOT RUNNING"
    echo "   → brew services start postgresql"
fi

# Check Qdrant
echo -n "Qdrant: "
if curl -s http://localhost:6333/health &> /dev/null; then
    COLLECTIONS=$(curl -s http://localhost:6333/collections | python3 -c "import sys,json; print(len(json.load(sys.stdin)['result']['collections']))" 2>/dev/null || echo "?")
    echo "✓ RUNNING ($COLLECTIONS collections)"
else
    echo "✗ NOT RUNNING"
    echo "   → docker run -p 6333:6333 qdrant/qdrant"
fi

# Check existing training data
echo ""
echo "Existing Training Data:"
if [ -d "training/data" ]; then
    for f in training/data/*.jsonl; do
        if [ -f "$f" ]; then
            COUNT=$(wc -l < "$f" | tr -d ' ')
            echo "  $(basename $f): $COUNT pairs"
        fi
    done
else
    echo "  No training data directory found"
fi

echo ""
echo "=============================================="
echo ""
echo "To run the pipeline:"
echo "  1. Set missing environment variables above"
echo "  2. Run: ./training/run-pipeline.sh pilot"
echo "  3. Review outputs, then: ./training/run-pipeline.sh all"
echo ""
