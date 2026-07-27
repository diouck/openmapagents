/**
 * icons.jsx — Bibliothèque d'icônes UNIQUE de l'application (Lucide React).
 *
 * Toutes les icônes de l'UI passent par ici : mêmes réglages (16 px, traits
 * fins arrondis) pour une charte graphique cohérente et professionnelle.
 * Ne pas utiliser d'emoji ni de SVG maison ailleurs — importer depuis ce module.
 */
import {
  MousePointer2, Ruler, Hexagon, CircleDot, Pencil, Navigation, Radar,
  Layers, BarChart3, Download, Blend, Database, Satellite, Server, Map as MapLucide,
  Mountain, Box, SquarePen, Film, Diff, GitCompare, MapPin, Leaf, Target,
  Printer, Upload, Link2, Sun, Moon, MessageSquare, X, Globe,
  ChevronLeft, ChevronRight, ChevronDown, ChevronUp, ChevronRight as CaretRight,
  Search, Plus, Minus, Calendar, TrendingUp, Triangle, Move, Grid2x2,
  Sprout, TreePine, Trees, Droplet, Droplets, Waves, Thermometer,
  CloudRain, Cloud, CloudFog, Wind, Building2, Flame, Wrench, Snowflake,
  Gauge, Factory, SunMedium, Sparkles, LandPlot, Route, Boxes,
  TrendingDown, Check, AlertTriangle, Palette, Loader2, Play, Pause, Save,
  Trash2, Settings, Info, Clipboard, RotateCcw, Eye, EyeOff, Maximize2,
  Crosshair, Layers3, FileDown, ZoomIn, Pipette, SlidersHorizontal, ScanLine,
  Image as ImageLucide, Filter, User, Lock, Send, Copy, ExternalLink, Hash,
  Utensils, ShoppingCart, ShoppingBag, Hospital, Landmark, Hotel, Square,
  File, Folder, Plug, Table, Spline, Undo2, Redo2,
  Lightbulb, Microscope, Rocket, Scissors, Shuffle, Grid3x3,
  MousePointerClick, Zap, RefreshCw, Bot, Wheat, Circle, Trophy,
  Fish, Anchor, Shell,
} from "lucide-react";

// Fabrique : applique les réglages par défaut, laisse surcharger via props.
const mk = (Comp, size = 16, strokeWidth = 1.75) =>
  function Icon(props) { return <Comp size={size} strokeWidth={strokeWidth} {...props} />; };

// ── Noms « rail / app » (compatibilité avec l'ancien système IcXxx) ──
export const IcArrow      = mk(MousePointer2);
export const IcRulerTool  = mk(Ruler);
export const IcHexagon    = mk(Hexagon);
export const IcCircleDot  = mk(CircleDot);
export const IcPencil     = mk(Pencil);
export const IcNavigation = mk(Navigation);
export const IcRadar      = mk(Radar);
export const IcStack      = mk(Layers);
export const IcBarChart   = mk(BarChart3);
export const IcArrowDown  = mk(Download);
export const IcVenn       = mk(Blend);
export const IcDatabase   = mk(Database);
export const IcSatellite  = mk(Satellite);
export const IcServer     = mk(Server);
export const IcMountain   = mk(Mountain);
export const IcCube       = mk(Box);
export const IcEdit       = mk(SquarePen);
export const IcFilm       = mk(Film);
export const IcDiff       = mk(Diff);
export const IcCompare    = mk(GitCompare);
export const IcOSM        = mk(MapPin);
export const IcLeaf       = mk(Leaf);
export const IcClassif    = mk(Target);
export const IcPrint      = mk(Printer);
export const IcUpload     = mk(Upload);
export const IcShare      = mk(Link2);
export const IcSun        = mk(Sun);
export const IcMoon       = mk(Moon);
export const IcChat       = mk(MessageSquare);
export const IcX          = mk(X);
export const IcGlobe      = mk(Globe);

// ── Navigation / contrôles ──
export const IcChevronLeft  = mk(ChevronLeft);
export const IcChevronRight = mk(ChevronRight);
export const IcChevronDown  = mk(ChevronDown);
export const IcChevronUp    = mk(ChevronUp);
export const IcCaretRight   = mk(CaretRight, 14);
export const IcSearch       = mk(Search);
export const IcPlus         = mk(Plus);
export const IcMinus        = mk(Minus);
export const IcCalendar     = mk(Calendar);
export const IcTrendingUp   = mk(TrendingUp);
export const IcTriangle     = mk(Triangle);
export const IcMove         = mk(Move, 14);
export const IcSparkles     = mk(Sparkles);

// ── Thèmes & indicateurs (menuTree, modals) ──
export const IcSprout      = mk(Sprout);
export const IcTreePine    = mk(TreePine);
export const IcTrees       = mk(Trees);
export const IcDroplet     = mk(Droplet);
export const IcDroplets    = mk(Droplets);
export const IcWaves       = mk(Waves);
export const IcThermometer = mk(Thermometer);
export const IcCloudRain   = mk(CloudRain);
export const IcCloud       = mk(Cloud);
export const IcCloudFog    = mk(CloudFog);
export const IcWind        = mk(Wind);
export const IcBuilding    = mk(Building2);
export const IcFlame       = mk(Flame);
export const IcWrench      = mk(Wrench);
export const IcSnowflake   = mk(Snowflake);
export const IcGauge       = mk(Gauge);
export const IcFactory     = mk(Factory);
export const IcSunMedium   = mk(SunMedium);
export const IcLandPlot    = mk(LandPlot);
export const IcRoute       = mk(Route);
export const IcBoxes       = mk(Boxes);
export const IcGrid        = mk(Grid2x2);
export const IcMapPin      = mk(MapPin);
export const IcMap         = mk(MapLucide);

// ── États, actions, contrôles (panneaux) ──
export const IcTrendingDown = mk(TrendingDown);
export const IcCheck        = mk(Check);
export const IcAlert        = mk(AlertTriangle);
export const IcPalette      = mk(Palette);
export const IcLoader       = mk(Loader2);
export const IcPlay         = mk(Play);
export const IcPause        = mk(Pause);
export const IcSave         = mk(Save);
export const IcTrash        = mk(Trash2);
export const IcSettings     = mk(Settings);
export const IcInfo         = mk(Info);
export const IcClipboard    = mk(Clipboard);
export const IcRefresh      = mk(RotateCcw);
export const IcEye          = mk(Eye);
export const IcEyeOff       = mk(EyeOff);
export const IcMaximize     = mk(Maximize2);
export const IcCrosshair    = mk(Crosshair);
export const IcLayers3      = mk(Layers3);
export const IcFileDown     = mk(FileDown);
export const IcZoomIn       = mk(ZoomIn);
export const IcPipette      = mk(Pipette);
export const IcSliders      = mk(SlidersHorizontal);
export const IcScanLine     = mk(ScanLine);
export const IcImage        = mk(ImageLucide);
export const IcFilter       = mk(Filter);
export const IcUser         = mk(User);
export const IcLock         = mk(Lock);
export const IcSend         = mk(Send);
export const IcCopy         = mk(Copy);
export const IcExternalLink = mk(ExternalLink);
export const IcHash         = mk(Hash);
export const IcUtensils     = mk(Utensils);
export const IcCart         = mk(ShoppingCart);
export const IcBag          = mk(ShoppingBag);
export const IcHospital     = mk(Hospital);
export const IcLandmark     = mk(Landmark);
export const IcHotel        = mk(Hotel);
export const IcSquare       = mk(Square);
export const IcFile         = mk(File);
export const IcFolder       = mk(Folder);
export const IcPlug         = mk(Plug);
export const IcTable        = mk(Table);
export const IcSpline       = mk(Spline);
export const IcUndo         = mk(Undo2);
export const IcRedo         = mk(Redo2);
export const IcBulb         = mk(Lightbulb);
export const IcMicroscope   = mk(Microscope);
export const IcRocket       = mk(Rocket);
export const IcScissors     = mk(Scissors);
export const IcShuffle      = mk(Shuffle);
export const IcGrid3        = mk(Grid3x3);
export const IcMouse        = mk(MousePointerClick);
export const IcZap          = mk(Zap);
export const IcRefreshCw    = mk(RefreshCw);
export const IcBot          = mk(Bot);
export const IcWheat        = mk(Wheat);
export const IcCircle       = mk(Circle);
export const IcTrophy       = mk(Trophy);
export const IcFish         = mk(Fish);
export const IcAnchor       = mk(Anchor);
export const IcShell        = mk(Shell);
