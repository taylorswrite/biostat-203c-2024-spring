// Bruin Learn Resources re-direct //
window.onload = () => {
  
  if (window.location.pathname === '/resources') {
      window.location.replace('https://bruinlearn.ucla.edu/courses/288');
  }
};

// Atomic Search widget script //
var atomicSearchWidgetScript = document.createElement("script");
atomicSearchWidgetScript.src = "https://js.atomicsearchwidget.com/atomic_search_widget.js";
document.getElementsByTagName("head")[0].appendChild(atomicSearchWidgetScript);

////////////////////////////////////////////////////
// DESIGNPLUS CONFIG                            //
////////////////////////////////////////////////////
// Legacy
var DT_variables = {
        iframeID: '',
        // Path to the hosted USU Design Tools
        path: 'https://designtools.ciditools.com/',
        templateCourse: '97',
        // OPTIONAL: Button will be hidden from view until launched using shortcut keys
        hideButton: true,
    	 // OPTIONAL: Limit by course format
	     limitByFormat: false, // Change to true to limit by format
	     // adjust the formats as needed. Format must be set for the course and in this array for tools to load
	     formatArray: [
            'online',
            'on-campus',
            'blended'
        ],
        // OPTIONAL: Limit tools loading by users role
        limitByRole: false, // set to true to limit to roles in the roleArray
        // adjust roles as needed
        roleArray: [
            'student',
            'teacher',
            'admin'
        ],
        // OPTIONAL: Limit tools to an array of Canvas user IDs
        limitByUser: false, // Change to true to limit by user
        // add users to array (Canvas user ID not SIS user ID)
        userArray: [
            '1234',
            '987654'
        ],
        // OPTIONAL: Relocate Ally alternative formats dropdown and hide heading
        overrideAllyHeadings: true,
};

// New
DpPrimary = {
    lms: 'canvas',
    templateCourse: '175903',
    hideButton: true,
    hideLti: false,
    extendedCourse: '', // added in sub-account theme
    sharedCourse: '', // added from localStorage
    courseFormats: [],
    canvasRoles: [],
    canvasUsers: [],
    canvasCourseIds: [],
    plugins: [],
    excludedModules: [],
    includedModules: [],
    lang: 'en',
    defaultToLegacy: true,
    enableVersionSwitching: true,
    hideSwitching: false,
}

// merge with extended/shared customizations config
DpConfig = { ...DpPrimary, ...(window.DpConfig ?? {}) }

$(function () {
    const uriPrefix = (location.href.includes('.beta.')) ? 'beta.' : '';
    const toolsUri = (DpConfig.toolsUri) ? DpConfig.toolsUri : `https://${uriPrefix}designplus.ciditools.com/`;
    $.getScript(`${toolsUri}js/controller.js`);
});
////////////////////////////////////////////////////
// END DESIGNPLUS CONFIG                        //
////////////////////////////////////////////////////


////////////////////////////////////////////////////
// BLACKBOARD ALLY                                //
////////////////////////////////////////////////////

window.ALLY_CFG = {

    'baseUrl': 'https://prod.ally.ac',
    'clientId': 11854,
    'lti13Id': '148090000000000117'
};

$.getScript(ALLY_CFG.baseUrl + '/integration/canvas/ally.js');

////////////////////////////////////////////////////
// END BLACKBOARD ALLY                            //
////////////////////////////////////////////////////

////////////////////////////////////////////////////
// BLUE EVALUATIONS                               //
////////////////////////////////////////////////////

var BLUE_CANVAS_SETUP={connectorUrl:"https://bruin-experience.teaching.ucla.edu/eipBlueConnector/",canvasAPI:window.location.origin,domainName:"com.explorance",consumerID:"4ka4tx4nLdejNUrQDGL9zA==",defaultLanguage:"en-us"},BlueCanvasJS=document.createElement("script");BlueCanvasJS.setAttribute("type","text/javascript");BlueCanvasJS.setAttribute("src","https://bruin-experience.teaching.ucla.edu/eipBlueConnector//Scripts/Canvas/BlueCanvas.min.js");document.getElementsByTagName("head")[0].appendChild(BlueCanvasJS);