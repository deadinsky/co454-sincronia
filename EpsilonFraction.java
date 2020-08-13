public class EpsilonFraction {
    long numInt = 0;
    long numEps = 0;
    long denInt = 1;
    long denEps = 0; // assumption; one of denEps and denInt is non-zero

    public final static EpsilonFraction one = new EpsilonFraction(1);
    public final static EpsilonFraction zero = new EpsilonFraction(0);
    public final static EpsilonFraction max = new EpsilonFraction(Long.MAX_VALUE);

    EpsilonFraction(long lNumInt) {
        numInt = lNumInt;
    }

    EpsilonFraction(long lNumInt, long lNumEps) {
        numInt = lNumInt;
        numEps = lNumEps;
    }

    EpsilonFraction(long lNumInt, long lNumEps, long lDenInt) {
        numInt = lNumInt;
        numEps = lNumEps;
        denInt = lDenInt;
        this.reduceFraction();
    }

    EpsilonFraction(long lNumInt, long lNumEps, long lDenInt, long lDenEps) {
        numInt = lNumInt;
        numEps = lNumEps;
        denInt = lDenInt;
        denEps = lDenEps;
        this.reduceFraction();
    }

    public long gcd(long a, long b) { return b==0 ? a : gcd(b, a%b); }

    public void reduceFraction() {
        if (numInt == 0 && numEps == 0) {
            denInt = 1;
            denEps = 0;
            return;
        }
        if (denInt < 0) {
            numInt *= 1;
            numEps *= 1;
            denInt *= 1;
            denEps *= 1;
        }
        long gcd;
        if (numEps == 0 && denEps == 0 && numInt != 0) {
            gcd = gcd(numInt, denInt);
            numInt /= gcd;
            denInt /= gcd;
            return;
        }
        if (numInt == 0 && denEps == 0 && numEps != 0) {
            gcd = gcd(numEps, denInt);
            numEps /= gcd;
            denInt /= gcd;
            return;
        }
        if (numInt == denInt && numEps == denEps) {
            numInt = 1;
            numEps = 0;
            denInt = 1;
            denEps = 0;
            return;
        }
        long numGcd = (numInt == 0 ? numEps : gcd(numInt, numEps));
        if (denEps == 0) {
            gcd = gcd(numGcd, denInt);
            numInt /= gcd;
            numEps /= gcd;
            denInt /= gcd;
            return;
        }
        if (denInt == 0) {
            gcd = gcd(numGcd, denEps);
            numInt /= gcd;
            numEps /= gcd;
            denEps /= gcd;
            return;
        }
        long denGcd = gcd(denInt, denEps);
        if (numInt / numGcd == denInt / denGcd && numEps / numGcd == denEps / denGcd) {
            gcd = gcd(numGcd, denGcd);
            numInt = numGcd / gcd;
            numEps = 0;
            denInt = denGcd / gcd;
            denEps = 0;
            return;
        }
        return;
    }

    public static EpsilonFraction addFractions(long x, EpsilonFraction y) {
        return new EpsilonFraction(x * y.denInt + y.numInt, x * y.denEps + y.numEps,
                y.denInt, y.denEps);
    }

    public static EpsilonFraction addFractions(EpsilonFraction x, long y) {
        return new EpsilonFraction(x.numInt + y * x.denInt, x.numEps + y * x.denEps,
                x.denInt, x.denEps);
    }

    public static EpsilonFraction addFractions(EpsilonFraction x, EpsilonFraction y) {
        if (x.denInt == y.denInt && x.denEps == y.denInt) {
            return new EpsilonFraction(x.numInt + y.numInt,
                    x.numEps + y.numEps, x.denInt, x.denEps);
        }
        if (x.denEps == 0 && y.denEps == 0) {
            return new EpsilonFraction(x.numInt * y.denInt + y.numInt * x.denInt,
                    x.numEps * y.denInt + y.numEps * x.denInt, x.denInt * y.denInt);
        }
        return new EpsilonFraction(x.numInt * y.denInt + y.numInt * x.denInt,
                x.numEps * y.denInt + y.numEps * x.denInt + x.numInt * y.denEps + y.numInt * x.denEps,
                x.denInt * y.denInt, x.denInt * y.denEps + y.denInt * x.denEps); //e^2 is ignored
    }

    public static EpsilonFraction subtractFractions(long x, EpsilonFraction y) {
        return new EpsilonFraction(x * y.denInt - y.numInt, x * y.denEps - y.numEps,
                y.denInt, y.denEps);
    }

    public static EpsilonFraction subtractFractions(EpsilonFraction x, long y) {
        return new EpsilonFraction(x.numInt - y * x.denInt, x.numEps - y * x.denEps,
                x.denInt, x.denEps);
    }

    public static EpsilonFraction subtractFractions(EpsilonFraction x, EpsilonFraction y) {
        if (x.denInt == y.denInt && x.denEps == y.denInt) {
            return new EpsilonFraction(x.numInt - y.numInt,
                    x.numEps - y.numEps, x.denInt, x.denEps);
        }
        if (x.denEps == 0 && y.denEps == 0) {
            return new EpsilonFraction(x.numInt * y.denInt - y.numInt * x.denInt,
                    x.numEps * y.denInt - y.numEps * x.denInt, x.denInt * y.denInt);
        }
        return new EpsilonFraction(x.numInt * y.denInt - y.numInt * x.denInt,
                x.numEps * y.denInt - y.numEps * x.denInt + x.numInt * y.denEps - y.numInt * x.denEps,
                x.denInt * y.denInt, x.denInt * y.denEps + y.denInt * x.denEps); //e^2 is ignored
    }

    public static EpsilonFraction multiplyFractions(long x, EpsilonFraction y) {
        return new EpsilonFraction(x * y.numInt, x * y.numEps, y.denInt, y.denEps);
    }

    public static EpsilonFraction multiplyFractions(EpsilonFraction x, long y) {
        return new EpsilonFraction(x.numInt * y, x.numEps * y, x.denInt, x.denEps);
    }

    public static EpsilonFraction multiplyFractions(EpsilonFraction x, EpsilonFraction y) {
        return new EpsilonFraction(x.numInt * y.numInt, x.numEps * y.numInt + y.numEps * x.numInt,
                x.denInt * y.denInt, x.denInt * y.denEps + y.denInt * x.denEps); //e^2 is ignored
    }

    public static EpsilonFraction divideFractions(long x, EpsilonFraction y) {
        return new EpsilonFraction(x * y.denInt, x * y.denEps, y.numInt, y.numEps);
    }

    public static EpsilonFraction divideFractions(EpsilonFraction x, long y) {
        return new EpsilonFraction(x.numInt, x.numEps, x.denInt * y, x.denEps * y);
    }

    public static EpsilonFraction divideFractions(EpsilonFraction x, EpsilonFraction y) {
        if (x.denInt == y.denInt && x.denEps == y.denEps) {
            return new EpsilonFraction(x.numInt, x.numEps, y.numInt, y.numEps);
        }
        return new EpsilonFraction(x.numInt * y.denInt, x.numEps * y.denInt + y.denEps * x.numInt,
                x.denInt * y.numInt, x.denInt * y.numEps + y.numInt * x.denEps); //e^2 is ignored
    }

    public boolean isEqual(EpsilonFraction x) {
        return (this.numInt == x.numInt && this.numEps == x.numEps &&
                this.denInt == x.denInt && this.denEps == x.denEps);
    }

    public boolean isEqual(long x) {
        return (this.numInt == x && this.numEps == 0 && this.denInt == 1 && this.denEps == 0);
    }

    public boolean isGreater(EpsilonFraction x) {
        if (denInt == x.denInt && denEps == x.denEps) {
            return (numInt > x.numInt || (numInt == x.numInt && numEps > x.numEps));
        }
        if (denInt < numInt || (denInt == x.denInt && denEps < x.denEps)) {
            return (numInt > x.numInt || (numInt == x.numInt && numEps >= x.numEps));
        }
        EpsilonFraction tmp = divideFractions(this, x);
        return (tmp.numInt > tmp.denInt || (tmp.numInt == tmp.denInt && tmp.numEps > tmp.denEps));
    }
}
